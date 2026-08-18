package e2e

// Nydus FUSE registry-backend fault-injection END-TO-END test.
//
// A toxiproxy-style fault-injecting TCP proxy sits between the nydus FUSE
// daemon and a real OCI registry (a throwaway local `registry:2` container).
// The daemon's registry backend is pointed at the proxy, and the proxy
// injects the classic network faults a production registry path suffers:
//
//	latency     — delayed first byte plus throttled response streaming
//	resets      — deterministic RST at accept of the first N connections
//	              after a fault set is applied (pooled connections are killed
//	              too, so the client is guaranteed to run into them)
//	truncation  — every connection's response stream is cut with an RST
//	              after a byte budget, killing whichever response is in flight
//
// Faults are deterministic (counter/budget-based, not probabilistic) so every
// CI run exercises the same schedule and a pass is never a lucky roll of the
// dice.
//
// The proxy runs inside the test process, so the test process itself must
// NEVER read from the FUSE mount: a blocked in-process FUSE read starves the
// Go runtime and the proxy with it, deadlocking the daemon's backend fetch
// against the read that triggered it (the same in-process starvation that
// forces the uffd suite to re-exec fault readers). Every mount read below is
// therefore performed by a child process (sha256sum); only metadata
// operations (walk/stat), which the daemon serves from the local bootstrap
// without touching the backend, happen in-process.
//
// Scenarios (ordered subtests, each starting from a COLD blob cache):
//
//	S1 baseline    proxy relays faithfully: full-tree sha256 == source
//	S2 latency     slow first byte + throttled body: reads stay byte-exact
//	               with NO application-level retries (client timeouts absorb it)
//	S3 resets      the first 2 connections after the fault set is applied are
//	               RST at accept: the backend's retry middleware masks it,
//	               reads stay byte-exact with NO application-level retries
//	S4 truncation  every connection dies after 8MiB of response bytes:
//	               mid-body EOF may surface EIO to a reader, but the cache slot
//	               stays retryable — bounded re-reads converge byte-exact
//	S5 chaos       latency + resets + truncation together under concurrent
//	               readers: bounded re-reads converge byte-exact
//	S6 recovery    faults lifted, cold restart: strict full-tree byte-exact,
//	               and the daemon never panicked across the whole run
//
// Every scenario also asserts the daemon process is still alive and the
// mountpoint is still live, and the fault scenarios assert the proxy actually
// injected faults so a pass can never be vacuous.
//
// Requirements: root (FUSE mount), docker (throwaway local registry), the
// nydus + nydusify binaries.
//
// Environment overrides:
//
//	FAULT_REGISTRY   registry host:port (default: 127.0.0.1:5000)
//	FAULT_REPO       repository        (default: nydus-e2e/fault-injection)
//	FAULT_TAG        image tag         (default: v1)

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------------------------------------------------------ fault proxy ----

// faultToxics is the active fault set of a faultProxy. The zero value injects
// nothing (faithful relay).
type faultToxics struct {
	// connectDelay delays the first byte relayed from the upstream, emulating
	// slow connection establishment / time-to-first-byte.
	connectDelay time.Duration
	// chunkDelay throttles the response stream: it is slept before every
	// relayed chunk (up to 32KiB) of upstream data.
	chunkDelay time.Duration
	// resetFirst RSTs the first N connections accepted after this toxic set
	// becomes active, before any byte is relayed; later connections pass (0
	// disables). Because set() also kills pooled keep-alive connections, the
	// client is guaranteed to hit the resets on its next fetch no matter how
	// aggressively it pools. Keep N below the backend's retry budget so a
	// single request always converges within its middleware retries.
	resetFirst uint64
	// truncateAfter cuts EVERY connection with an RST once this many upstream
	// response bytes have been relayed on it (0 disables), truncating whichever
	// response is in flight. Keep the budget well above the backend's largest
	// single range fetch so a retried fetch on a fresh connection can always
	// complete, guaranteeing forward progress.
	truncateAfter int64
}

// faultProxy is a minimal toxiproxy-style fault-injecting TCP proxy. It
// listens on a loopback port, relays every accepted connection to the fixed
// upstream, and applies the currently configured faultToxics. Toxics can be
// swapped at any time; connections pick up the set active when they are
// accepted.
type faultProxy struct {
	listener net.Listener
	upstream string

	mu     sync.Mutex
	toxics faultToxics
	active map[net.Conn]struct{} // live connections, force-closed by stop()

	conns       atomic.Uint64 // accepted connections (lifetime total)
	epochConns  atomic.Uint64 // accepted connections since the last set()
	resets      atomic.Uint64 // RSTs injected at accept
	truncations atomic.Uint64 // response streams cut short
	relayed     atomic.Int64  // upstream->client bytes actually relayed

	closed chan struct{}
	wg     sync.WaitGroup
}

// startFaultProxy listens on an ephemeral loopback port and starts relaying
// to upstream (host:port). The proxy is registered for cleanup with t.
func startFaultProxy(t *testing.T, upstream string) *faultProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "fault proxy failed to listen")
	p := &faultProxy{
		listener: listener,
		upstream: upstream,
		active:   make(map[net.Conn]struct{}),
		closed:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.stop)
	t.Logf("fault proxy %s -> %s", p.addr(), upstream)
	return p
}

func (p *faultProxy) addr() string { return p.listener.Addr().String() }

func (p *faultProxy) stop() {
	select {
	case <-p.closed:
		return
	default:
	}
	close(p.closed)
	_ = p.listener.Close()
	// Force-close live connections (idle keep-alive connections would
	// otherwise keep their relay goroutines blocked in Read forever).
	p.mu.Lock()
	for conn := range p.active {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// track registers a live connection for force-close on stop(). It returns
// false when the proxy is already stopping.
func (p *faultProxy) track(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.closed:
		return false
	default:
	}
	p.active[conn] = struct{}{}
	return true
}

func (p *faultProxy) untrack(conn net.Conn) {
	p.mu.Lock()
	delete(p.active, conn)
	p.mu.Unlock()
}

// set replaces the active fault set and force-closes every live connection
// (toxiproxy-style: pooled keep-alive connections would otherwise keep the
// previous fault behaviour indefinitely). New connections observe the new set.
func (p *faultProxy) set(toxics faultToxics) {
	p.mu.Lock()
	p.toxics = toxics
	p.epochConns.Store(0) // new toxic epoch: resetFirst counts from here
	conns := make([]net.Conn, 0, len(p.active))
	for conn := range p.active {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		if tcp, ok := conn.(*net.TCPConn); ok {
			rstClose(tcp)
		} else {
			_ = conn.Close()
		}
	}
}

func (p *faultProxy) snapshot() faultToxics {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.toxics
}

func (p *faultProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return // listener closed by stop()
		}
		p.wg.Add(1)
		go p.handle(conn.(*net.TCPConn))
	}
}

// rst aborts the client connection with an RST (SO_LINGER=0 close) so the
// peer observes "connection reset by peer" rather than a graceful FIN.
func rstClose(conn *net.TCPConn) {
	_ = conn.SetLinger(0)
	_ = conn.Close()
}

func (p *faultProxy) handle(client *net.TCPConn) {
	defer p.wg.Done()
	p.conns.Add(1)
	seq := p.epochConns.Add(1) // 1-based within the current toxic epoch
	toxics := p.snapshot()

	if toxics.resetFirst > 0 && seq <= toxics.resetFirst {
		p.resets.Add(1)
		rstClose(client)
		return
	}

	upstream, err := net.DialTimeout("tcp", p.upstream, 10*time.Second)
	if err != nil {
		rstClose(client)
		return
	}
	up := upstream.(*net.TCPConn)

	if !p.track(client) || !p.track(up) {
		_ = client.Close()
		_ = up.Close()
		return
	}
	defer p.untrack(client)
	defer p.untrack(up)

	truncate := toxics.truncateAfter > 0

	var once sync.Once
	kill := func() {
		once.Do(func() {
			rstClose(client)
			rstClose(up)
		})
	}

	done := make(chan struct{}, 2)

	// client -> upstream: requests are relayed untouched.
	go func() {
		defer func() { done <- struct{}{} }()
		_, _ = io.Copy(up, client)
		_ = up.CloseWrite()
	}()

	// upstream -> client: the fault-injected response direction.
	go func() {
		defer func() { done <- struct{}{} }()
		p.relayResponses(up, client, toxics, truncate, kill)
	}()

	<-done
	<-done
	_ = client.Close()
	_ = up.Close()
}

// relayResponses copies upstream data to the client, applying the latency and
// truncation toxics. kill hard-closes both connections when the truncation
// byte budget is exhausted.
func (p *faultProxy) relayResponses(up, client *net.TCPConn, toxics faultToxics, truncate bool, kill func()) {
	buf := make([]byte, 32*1024)
	var relayed int64
	first := true
	for {
		n, err := up.Read(buf)
		if n > 0 {
			if first {
				first = false
				if toxics.connectDelay > 0 {
					time.Sleep(toxics.connectDelay)
				}
			}
			if toxics.chunkDelay > 0 {
				time.Sleep(toxics.chunkDelay)
			}
			chunk := buf[:n]
			if truncate && relayed+int64(n) > toxics.truncateAfter {
				keep := toxics.truncateAfter - relayed
				if keep < 0 {
					keep = 0
				}
				if keep > 0 {
					_, _ = client.Write(chunk[:keep])
					p.relayed.Add(keep)
				}
				p.truncations.Add(1)
				kill()
				return
			}
			if _, werr := client.Write(chunk); werr != nil {
				return
			}
			relayed += int64(n)
			p.relayed.Add(int64(n))
		}
		if err != nil {
			_ = client.CloseWrite()
			return
		}
	}
}

// ------------------------------------------------------------ test fixture ----

type faultInjectionEnv struct {
	daemonProc

	registry string
	repo     string
	tag      string
	imageRef string

	nydusBin    string
	nydusifyBin string

	workDir    string
	sourceDir  string
	cacheDir   string
	mntDir     string
	logDir     string
	checkDir   string
	configPath string
	bootstrap  string

	sourceHashes map[string]string
	registryCID  string

	proxy *faultProxy
}

func TestFuseFaultInjection(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("FUSE fault-injection E2E requires Linux")
	}
	if os.Getuid() != 0 {
		t.Skip("FUSE mounts require root")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker required for the throwaway local registry")
	}

	e := &faultInjectionEnv{
		registry: envOr("FAULT_REGISTRY", "127.0.0.1:5000"),
		repo:     envOr("FAULT_REPO", "nydus-e2e/fault-injection"),
		tag:      envOr("FAULT_TAG", "v1"),
	}
	e.imageRef = fmt.Sprintf("%s/%s:%s", e.registry, e.repo, e.tag)
	e.nydusBin = lookupBinFromEnv(t, "NYDUS_BIN", "nydus")
	e.nydusifyBin = lookupBinFromEnv(t, "NYDUSIFY_BIN", "nydusify")

	e.workDir = t.TempDir()
	e.sourceDir = filepath.Join(e.workDir, "source")
	e.cacheDir = filepath.Join(e.workDir, "cache")
	e.mntDir = filepath.Join(e.workDir, "mnt")
	e.logDir = filepath.Join(e.workDir, "logs")
	e.checkDir = filepath.Join(e.workDir, "check-output")
	e.configPath = filepath.Join(e.workDir, "config-registry.yaml")
	e.daemonLog = filepath.Join(e.workDir, "daemon.console.log")
	e.bootstrap = filepath.Join(e.checkDir, "target", "bootstrap", "image", "image.boot")

	t.Cleanup(func() { e.cleanup(t) })

	e.startLocalRegistry(t)
	e.buildDataset(t)
	e.convertAndExport(t)

	// The proxy fronts the registry; the daemon only ever talks to the proxy.
	e.proxy = startFaultProxy(t, e.registry)
	e.writeConfig(t)
	e.startDaemonCold(t)

	// Ordered: each scenario restarts the daemon COLD so the registry backend
	// is exercised, and S6 depends on the toxics being lifted after S5.
	t.Run("S1_baseline", e.caseBaseline)
	t.Run("S2_latency", e.caseLatency)
	t.Run("S3_connection_resets", e.caseConnectionResets)
	t.Run("S4_truncation", e.caseTruncation)
	t.Run("S5_chaos", e.caseChaos)
	t.Run("S6_recovery", e.caseRecovery)
}

// ------------------------------------------------------------------ setup ----

func (e *faultInjectionEnv) startLocalRegistry(t *testing.T) {
	t.Helper()
	host := e.registry
	loopback := strings.HasPrefix(host, "127.0.0.1:") ||
		strings.HasPrefix(host, "localhost:") ||
		strings.HasPrefix(host, "[::1]:")
	if !loopback {
		t.Logf("using external registry %s (not auto-starting one)", host)
		return
	}
	if registryReady(host) {
		t.Logf("registry already listening at %s", host)
		return
	}
	port := host[strings.LastIndex(host, ":")+1:]
	t.Logf("starting throwaway registry:2 on %s", host)
	out, err := exec.Command("docker", "run", "-d", "-p", port+":5000", "--restart=no", "registry:2").CombinedOutput()
	require.NoError(t, err, "failed to start local registry container: %s", out)
	e.registryCID = strings.TrimSpace(string(out))

	require.Eventually(t, func() bool {
		return registryReady(host)
	}, 30*time.Second, time.Second, "local registry did not become ready")
	t.Log("  registry up")
}

func (e *faultInjectionEnv) buildDataset(t *testing.T) {
	t.Helper()
	t.Log("building dataset")
	for _, d := range []string{
		filepath.Join(e.sourceDir, "subdir"),
		filepath.Join(e.sourceDir, "many"),
		e.cacheDir, e.mntDir, e.logDir,
	} {
		require.NoError(t, os.MkdirAll(d, 0755))
	}

	require.NoError(t, os.WriteFile(filepath.Join(e.sourceDir, "hello.txt"), []byte("hello nydus fault injection\n"), 0644))
	writeRandomFile(t, filepath.Join(e.sourceDir, "data.bin"), 100*4096)      // ~400K
	writeRandomFile(t, filepath.Join(e.sourceDir, "large.bin"), 32*1024*1024) // 32M
	require.NoError(t, os.WriteFile(filepath.Join(e.sourceDir, "subdir", "nested.txt"), []byte("nested file content\n"), 0644))
	// a fan of small files for the concurrent chaos readers (deterministic sizes)
	for i := 1; i <= 16; i++ {
		size := (4 + (i*7)%20) * 4096
		writeRandomFile(t, filepath.Join(e.sourceDir, "many", fmt.Sprintf("f%d.bin", i)), size)
	}

	e.sourceHashes = hashTree(t, e.sourceDir)
	t.Logf("  %d source files hashed", len(e.sourceHashes))
}

// convertAndExport pushes the nydus image straight to the registry (NOT
// through the proxy — only the daemon's backend traffic is fault-injected)
// and exports the bootstrap via `nydusify check`.
func (e *faultInjectionEnv) convertAndExport(t *testing.T) {
	t.Helper()
	insecure := []string{"--target-insecure", "--target-plain-http"}

	t.Logf("nydusify convert -> %s", e.imageRef)
	args := append([]string{"convert",
		"--source", e.sourceDir,
		"--target", e.imageRef,
		"--builder", e.nydusBin,
	}, insecure...)
	out, err := exec.Command(e.nydusifyBin, args...).CombinedOutput()
	require.NoError(t, err, "nydusify convert failed:\n%s", out)

	t.Log("nydusify check -> export bootstrap")
	require.NoError(t, os.RemoveAll(e.checkDir))
	args = append([]string{"check",
		"--target", e.imageRef,
		"--work-dir", e.checkDir,
	}, insecure...)
	out, err = exec.Command(e.nydusifyBin, args...).CombinedOutput()
	require.NoError(t, err, "nydusify check failed:\n%s", out)

	if _, err := os.Stat(e.bootstrap); err != nil {
		if found := findFile(e.checkDir, "image.boot"); found != "" {
			e.bootstrap = found
		}
	}
	require.FileExists(t, e.bootstrap, "exported bootstrap not found")
}

// writeConfig points the registry backend at the fault proxy. Modest timeout
// and retry budgets keep the fault scenarios snappy while still letting the
// retry middleware absorb resets.
func (e *faultInjectionEnv) writeConfig(t *testing.T) {
	t.Helper()
	config := fmt.Sprintf(`backend:
  type: registry
  config:
    addr: http://%s
    repository: %s
    http:
      timeout: 30s
      max_retries: 4
      tls:
        skip_verify: true
storage:
  dir: %s
prefetch:
  scope: none
`, e.proxy.addr(), e.repo, e.cacheDir)
	require.NoError(t, os.WriteFile(e.configPath, []byte(config), 0644))
	t.Logf("  wrote %s (backend addr: http://%s)", e.configPath, e.proxy.addr())
}

// ----------------------------------------------------------------- daemon ----

// startDaemonCold wipes the blob cache and starts a fresh `nydus fuse` daemon
// against the proxy-fronted registry backend, waiting for the mountpoint.
// Startup (blob metadata recovery via HEAD + footer reads) happens with the
// toxics active at call time — scenarios set their toxics after this returns
// so a fault can never kill the daemon before it is mounted.
func (e *faultInjectionEnv) startDaemonCold(t *testing.T) {
	t.Helper()
	wipeCacheDir(e.cacheDir)
	t.Log("starting nydus fuse daemon (cold cache)")
	cmd := exec.Command(e.nydusBin, "fuse",
		"--bootstrap", e.bootstrap,
		"--config", e.configPath,
		"--mountpoint", e.mntDir,
		"--log-level", "debug",
		"--log-dir", e.logDir,
	)
	e.spawnDaemonCmd(t, cmd)

	require.Eventually(t, func() bool {
		select {
		case <-e.daemonExited:
			require.FailNowf(t, "daemon exited during startup", "%s", e.logCorpus())
		default:
		}
		return isMountpoint(e.mntDir)
	}, 60*time.Second, time.Second, "daemon did not mount within 60s:\n%s", e.logCorpus())

	// Prime read in the fault-free window: forces the lazy blob metadata
	// fetch (HEAD + footer range reads) through the proxy now, so a scenario's
	// faults hit steady-state data fetches rather than daemon bring-up.
	sum, retries := retryingSHA(t, filepath.Join(e.mntDir, "hello.txt"), 10)
	require.Equal(t, e.sourceHashes["hello.txt"], sum, "prime read byte-exact")
	t.Logf("  daemon ready (pid=%d, prime read retries: %d)", cmd.Process.Pid, retries)
}

func (e *faultInjectionEnv) restartDaemonColdQuiet(t *testing.T) {
	t.Helper()
	e.proxy.set(faultToxics{}) // fault-free window for shutdown + startup
	e.stopDaemonAndUnmount(t)
	e.startDaemonCold(t)
}

func (e *faultInjectionEnv) stopDaemonAndUnmount(t *testing.T) {
	t.Helper()
	e.stopDaemon(t)
	unmountFuse(e.mntDir)
}

func (e *faultInjectionEnv) cleanup(t *testing.T) {
	if e.proxy != nil {
		e.proxy.set(faultToxics{})
	}
	e.stopDaemonAndUnmount(t)
	if e.registryCID != "" {
		_ = exec.Command("docker", "rm", "-f", e.registryCID).Run()
	}
}

// assertHealthy asserts the daemon survived the scenario: process alive,
// mountpoint live.
func (e *faultInjectionEnv) assertHealthy(t *testing.T) {
	t.Helper()
	select {
	case <-e.daemonExited:
		require.FailNowf(t, "daemon died during the scenario", "%s", e.logCorpus())
	default:
	}
	assert.True(t, isMountpoint(e.mntDir), "mountpoint is still live")
}

// ------------------------------------------------------------ read helpers ----
//
// Every DATA read of the FUSE mount runs in a child process (sha256sum): an
// in-process read would block the test binary in the kernel while the daemon
// fetches through the proxy — which lives in this very process — deadlocking
// the fetch against the read that triggered it. Metadata (walk/stat) is served
// from the local bootstrap without backend traffic and is safe in-process.

// trySHA hashes one file in a child process, returning an error when the
// child fails (e.g. EIO from a faulted cold read).
func trySHA(path string) (string, error) {
	out, err := exec.Command("sha256sum", path).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("sha256sum %s: %w: %s", path, err, exitErr.Stderr)
		}
		return "", fmt.Errorf("sha256sum %s: %w", path, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("sha256sum %s: empty output", path)
	}
	return fields[0], nil
}

// mustSHA hashes one file in a child process with NO retries; any read error
// fails the test immediately (used when faults must be fully masked by the
// backend).
func mustSHA(t *testing.T, path string) string {
	t.Helper()
	sum, err := trySHA(path)
	require.NoError(t, err, "strict read of %s failed", path)
	return sum
}

// retryingSHA hashes one file with bounded retries. Truncation faults may
// surface a mid-body EOF as EIO on a cold read; the cache slot stays
// retryable and fetched groups stay cached, so re-reads make forward
// progress and must converge.
func retryingSHA(t *testing.T, path string, attempts int) (string, int) {
	t.Helper()
	var lastErr error
	for i := 1; i <= attempts; i++ {
		sum, err := trySHA(path)
		if err == nil {
			return sum, i - 1
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	require.NoErrorf(t, lastErr, "read of %s did not converge after %d attempts", path, attempts)
	return "", attempts
}

// mountRelPaths walks the mount METADATA-only (safe in-process: lookups are
// served from the local bootstrap) and returns the relative path of every
// regular file, asserting the file set matches the source exactly.
func (e *faultInjectionEnv) mountRelPaths(t *testing.T) []string {
	t.Helper()
	var rels []string
	err := filepath.Walk(e.mntDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(e.mntDir, path)
		if err != nil {
			return err
		}
		rels = append(rels, rel)
		return nil
	})
	require.NoError(t, err, "walking the mount")
	require.Len(t, rels, len(e.sourceHashes), "mount exposes exactly the source file set")
	return rels
}

// strictTreeHash hashes every mount file with NO application-level retries.
func (e *faultInjectionEnv) strictTreeHash(t *testing.T) map[string]string {
	t.Helper()
	hashes := make(map[string]string, len(e.sourceHashes))
	for _, rel := range e.mountRelPaths(t) {
		hashes[rel] = mustSHA(t, filepath.Join(e.mntDir, rel))
	}
	return hashes
}

// retryingTreeHash hashes every mount file with per-file retries and returns
// the map plus the total number of retries burned.
func (e *faultInjectionEnv) retryingTreeHash(t *testing.T, attempts int) (map[string]string, int) {
	t.Helper()
	hashes := make(map[string]string, len(e.sourceHashes))
	total := 0
	for _, rel := range e.mountRelPaths(t) {
		sum, retries := retryingSHA(t, filepath.Join(e.mntDir, rel), attempts)
		hashes[rel] = sum
		total += retries
	}
	return hashes, total
}

// =============================================================== scenarios ===

func (e *faultInjectionEnv) caseBaseline(t *testing.T) { // S1
	relayedBefore := e.proxy.relayed.Load()
	mntHashes := e.strictTreeHash(t)
	assert.Equal(t, e.sourceHashes, mntHashes, "faithful relay: full-tree sha256 == source")
	assert.Greater(t, e.proxy.relayed.Load(), relayedBefore, "reads went THROUGH the proxy")
	e.assertHealthy(t)
}

func (e *faultInjectionEnv) caseLatency(t *testing.T) { // S2
	e.restartDaemonColdQuiet(t)
	e.proxy.set(faultToxics{
		connectDelay: 200 * time.Millisecond,
		chunkDelay:   2 * time.Millisecond,
	})
	defer e.proxy.set(faultToxics{})

	// Strict reads: latency must be fully absorbed by the client timeouts.
	for _, rel := range []string{"data.bin", filepath.Join("many", "f1.bin"), filepath.Join("many", "f9.bin")} {
		got := mustSHA(t, filepath.Join(e.mntDir, rel))
		assert.Equal(t, e.sourceHashes[rel], got, "%s byte-exact under latency", rel)
	}
	e.assertHealthy(t)
}

func (e *faultInjectionEnv) caseConnectionResets(t *testing.T) { // S3
	e.restartDaemonColdQuiet(t)
	resetsBefore := e.proxy.resets.Load()
	// set() kills the pooled prime-read connection, so the next fetch must
	// reconnect and eat the 2 accept-time RSTs; 2 is safely below the
	// backend's retry budget (http.max_retries: 4), so the middleware masks
	// them without surfacing an error.
	e.proxy.set(faultToxics{resetFirst: 2})
	defer e.proxy.set(faultToxics{})

	// Strict reads: an RST before any response byte is a transport error the
	// backend's retry middleware must mask completely.
	mntHashes := e.strictTreeHash(t)
	assert.Equal(t, e.sourceHashes, mntHashes, "full-tree byte-exact across injected connection resets")
	assert.GreaterOrEqual(t, e.proxy.resets.Load(), resetsBefore+2, "the proxy actually injected resets")
	e.assertHealthy(t)
}

func (e *faultInjectionEnv) caseTruncation(t *testing.T) { // S4
	e.restartDaemonColdQuiet(t)
	truncationsBefore := e.proxy.truncations.Load()
	// Every connection dies with an RST after 8MiB of response bytes: reading
	// the ~33MiB tree guarantees several kills. The budget is far above the
	// backend's largest single range fetch (one ~1MiB chunk group), so a
	// retried fetch on a fresh connection always completes — forward progress
	// is guaranteed.
	e.proxy.set(faultToxics{truncateAfter: 8 * 1024 * 1024})
	defer e.proxy.set(faultToxics{})

	// A mid-body cut may surface EIO to the reader (a short body is never
	// silently accepted), but the cache slot stays retryable: bounded
	// re-reads must converge byte-exact.
	mntHashes, retries := e.retryingTreeHash(t, 10)
	assert.Equal(t, e.sourceHashes, mntHashes, "full-tree byte-exact under response truncation (with bounded re-reads)")
	assert.Greater(t, e.proxy.truncations.Load(), truncationsBefore, "the proxy actually truncated responses")
	t.Logf("  [S4] application-level read retries burned: %d", retries)
	e.assertHealthy(t)
}

func (e *faultInjectionEnv) caseChaos(t *testing.T) { // S5
	e.restartDaemonColdQuiet(t)
	e.proxy.set(faultToxics{
		connectDelay:  50 * time.Millisecond,
		chunkDelay:    time.Millisecond,
		resetFirst:    2,
		truncateAfter: 8 * 1024 * 1024,
	})
	defer e.proxy.set(faultToxics{})

	// Concurrent readers over the small-file fan while the large file is
	// hashed with retries: stresses parallel group fetches under faults. Each
	// reader is a child process, so a blocked FUSE read can never starve the
	// in-process proxy.
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 1; i <= 16; i++ {
		rel := filepath.Join("many", fmt.Sprintf("f%d.bin", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastErr error
			for attempt := 0; attempt < 10; attempt++ {
				sum, err := trySHA(filepath.Join(e.mntDir, rel))
				if err == nil {
					if sum != e.sourceHashes[rel] {
						lastErr = fmt.Errorf("%s hash mismatch under chaos", rel)
					} else {
						lastErr = nil
					}
					break
				}
				lastErr = err
				time.Sleep(300 * time.Millisecond)
			}
			if lastErr != nil {
				errs <- lastErr
			}
		}()
	}
	wg.Wait()
	close(errs)
	var readErrs []string
	for err := range errs {
		readErrs = append(readErrs, err.Error())
	}
	assert.Empty(t, readErrs, "every concurrent reader converged byte-exact under chaos")

	sum, retries := retryingSHA(t, filepath.Join(e.mntDir, "large.bin"), 20)
	assert.Equal(t, e.sourceHashes["large.bin"], sum, "large.bin byte-exact under chaos")
	t.Logf("  [S5] large.bin read retries burned: %d", retries)
	e.assertHealthy(t)
}

func (e *faultInjectionEnv) caseRecovery(t *testing.T) { // S6
	// Faults lifted, cold restart: everything must be strictly byte-exact
	// again, proving no fault left persistent damage (cache poisoning, a
	// wedged daemon, a dead mount).
	e.restartDaemonColdQuiet(t)
	mntHashes := e.strictTreeHash(t)
	assert.Equal(t, e.sourceHashes, mntHashes, "full-tree byte-exact after faults are lifted")
	e.assertHealthy(t)
	assert.Equal(t, 0, e.countLogs(`panic`), "the daemon never panicked across the whole run")

	t.Logf("  proxy totals: %d connections, %d resets, %d truncations, %d bytes relayed",
		e.proxy.conns.Load(), e.proxy.resets.Load(), e.proxy.truncations.Load(), e.proxy.relayed.Load())
}
