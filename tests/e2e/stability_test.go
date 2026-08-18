package e2e

import (
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dragonflyoss/nydus/tests/e2e/corpus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	defaultChurnSeconds       = 120
	defaultChurnMinIterations = 30
	stabilityReadTimeout      = 10 * time.Second
)

type stabilityFixture struct {
	srcDir     string
	bootstrap  string
	blobDir    string
	mountpoint string
}

func TestStability(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}

	nydusBin := mustLookupExecutable(t, "nydus")

	t.Run("MountUnmountChurn", func(t *testing.T) {
		testStabilityMountUnmountChurn(t, nydusBin)
	})
	t.Run("KillUnderIO", func(t *testing.T) {
		testStabilityKillUnderIO(t, nydusBin)
	})
	t.Run("DiskFullCacheDir", func(t *testing.T) {
		testStabilityDiskFullCacheDir(t, nydusBin)
	})
	t.Run("FdUlimit", func(t *testing.T) {
		testStabilityFdUlimit(t, nydusBin)
	})
}

func testStabilityMountUnmountChurn(t *testing.T, nydusBin string) {
	root := t.TempDir()
	fixture := newStabilityFixture(t, nydusBin, root, func(c *corpus.Corpus) {
		c.CreateFile(t, "stability/churn.txt", []byte("mount churn sentinel\n"))
	})

	refs := map[string]string{
		"files/tiny_2b":          filepath.Join(fixture.srcDir, "files/tiny_2b"),
		"files/small_100b":       filepath.Join(fixture.srcDir, "files/small_100b"),
		"stability/churn.txt":    filepath.Join(fixture.srcDir, "stability/churn.txt"),
		"files/byte_pattern":     filepath.Join(fixture.srcDir, "files/byte_pattern"),
		"files/just_under_block": filepath.Join(fixture.srcDir, "files/just_under_block"),
	}

	deadline := time.Now().Add(time.Duration(envInt(t, "NYDUS_E2E_CHURN_SECS", defaultChurnSeconds)) * time.Second)
	minIterations := envInt(t, "NYDUS_E2E_CHURN_MIN_ITERATIONS", defaultChurnMinIterations)
	startMounts := mountSourceCount(t, "nydus")

	var procs []*exec.Cmd
	iterations := 0
	for time.Now().Before(deadline) {
		cmd, cleanup := startStabilityMount(t, nydusBin, fixture.bootstrap, fixture.blobDir, "", fixture.mountpoint)
		procs = append(procs, cmd)
		for rel, want := range refs {
			got := filepath.Join(fixture.mountpoint, rel)
			requireFileContentEqual(t, want, got)
		}
		cleanup()
		require.False(t, isMountpoint(fixture.mountpoint), "mountpoint leaked after iteration %d", iterations)
		require.NotNil(t, cmd.ProcessState, "nydus process %d was not reaped", iterations)
		require.True(t, cmd.ProcessState.Exited(), "nydus process %d did not exit", iterations)
		iterations++
	}

	require.GreaterOrEqual(t, iterations, minIterations, "completed too few churn iterations before deadline")
	require.False(t, isMountpoint(fixture.mountpoint), "mountpoint should be detached after churn")
	require.Equal(t, startMounts, mountSourceCount(t, "nydus"), "nydus mount count should return to baseline")
	for i, cmd := range procs {
		require.NotNil(t, cmd.ProcessState, "iteration %d process was not reaped", i)
	}
}

func testStabilityKillUnderIO(t *testing.T, nydusBin string) {
	root := t.TempDir()
	fixture := newStabilityFixture(t, nydusBin, root, func(c *corpus.Corpus) {
		c.CreateRandomFile(t, "stability/io-big.bin", 12<<20)
	})

	cmd, cleanup := startStabilityMount(t, nydusBin, fixture.bootstrap, fixture.blobDir, "", fixture.mountpoint)
	defer cleanup()

	type readerResult struct {
		name string
		err  error
	}

	paths := []string{
		filepath.Join(fixture.mountpoint, "stability/io-big.bin"),
		filepath.Join(fixture.mountpoint, "files/large_256k"),
		filepath.Join(fixture.mountpoint, "files/byte_pattern"),
	}

	const readers = 8
	var started atomic.Int32
	results := make(chan readerResult, readers)
	for i := range readers {
		go func(id int) {
			path := paths[id%len(paths)]
			results <- readerResult{name: filepath.Base(path), err: churnReadUntilFailure(path, id, &started)}
		}(i)
	}

	require.Eventually(t, func() bool {
		return started.Load() == readers
	}, 10*time.Second, 50*time.Millisecond, "readers never got in flight")

	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))

	deadline := time.After(stabilityReadTimeout)
	for i := 0; i < readers; i++ {
		select {
		case res := <-results:
			require.Error(t, res.err, "reader %s should stop with an error once the daemon dies", res.name)
		case <-deadline:
			t.Fatal("reader goroutines stayed blocked after SIGKILL")
		}
	}

	unmountFuse(fixture.mountpoint)
	require.Eventually(t, func() bool {
		return !isMountpoint(fixture.mountpoint)
	}, 5*time.Second, 100*time.Millisecond, "stale mountpoint did not go away")
	cleanup()

	_, recoveryCleanup := startStabilityMount(t, nydusBin, fixture.bootstrap, fixture.blobDir, "", fixture.mountpoint)
	defer recoveryCleanup()
	requireFileContentEqual(t,
		filepath.Join(fixture.srcDir, "files/tiny_2b"),
		filepath.Join(fixture.mountpoint, "files/tiny_2b"),
	)
}

func testStabilityDiskFullCacheDir(t *testing.T, nydusBin string) {
	root := t.TempDir()
	cacheFS := filepath.Join(root, "cachefs")
	require.NoError(t, os.MkdirAll(cacheFS, 0o755))

	out, err := exec.Command("mount", "-t", "tmpfs", "-o", "size=4m", "tmpfs", cacheFS).CombinedOutput()
	require.NoError(t, err, "mount tmpfs cachefs failed: %s", out)
	t.Cleanup(func() {
		_ = exec.Command("umount", "-l", cacheFS).Run()
	})

	cacheDir := filepath.Join(cacheFS, "cache")
	fixture := newStabilityFixture(t, nydusBin, root, func(c *corpus.Corpus) {
		c.CreateRandomFile(t, "cache/big-a.bin", 8<<20)
		c.CreateRandomFile(t, "cache/big-b.bin", 8<<20)
		c.CreateRandomFile(t, "cache/big-c.bin", 8<<20)
	})

	cmd, cleanup := startStabilityMount(t, nydusBin, fixture.bootstrap, fixture.blobDir, cacheDir, fixture.mountpoint)
	defer cleanup()

	var readErr error
	for i := 0; i < 6 && readErr == nil && tmpfsAvailBytes(t, cacheFS) > 128<<10; i++ {
		for _, rel := range []string{"cache/big-a.bin", "cache/big-b.bin", "cache/big-c.bin"} {
			_, readErr = os.ReadFile(filepath.Join(fixture.mountpoint, rel))
			if readErr != nil {
				break
			}
			if tmpfsAvailBytes(t, cacheFS) <= 128<<10 {
				break
			}
		}
	}

	require.True(t, readErr != nil || tmpfsAvailBytes(t, cacheFS) <= 128<<10,
		"cache tmpfs never filled and reads never errored; still have %d bytes free", tmpfsAvailBytes(t, cacheFS))
	require.True(t, processAlive(cmd), "daemon should stay alive when cache backing store fills")
	require.NoError(t, statWithin(filepath.Join(fixture.mountpoint, "files/tiny_2b"), 5*time.Second))
}

func testStabilityFdUlimit(t *testing.T, nydusBin string) {
	prlimit, err := exec.LookPath("prlimit")
	if err != nil {
		t.Skip("prlimit not available")
	}

	root := t.TempDir()
	fixture := newStabilityFixture(t, nydusBin, root, func(c *corpus.Corpus) {
		for i := 0; i < 128; i++ {
			c.CreateRandomFile(t, fmt.Sprintf("fanout/%03d.bin", i), 32<<10)
		}
	})

	args := append([]string{"--nofile=64:64", nydusBin}, nydusFuseArgs(fixture.bootstrap, fixture.blobDir, fixture.mountpoint)...)
	prepareMountpoint(t, fixture.mountpoint)
	cmd := exec.Command(prlimit, args...)
	cleanup := startFuseMount(t, cmd, fixture.mountpoint, "nydus prlimit fuse")
	defer cleanup()

	files := make([]string, 0, 96)
	for i := 0; i < 96; i++ {
		files = append(files, fmt.Sprintf("fanout/%03d.bin", i))
	}

	hold := make(chan struct{})
	errs := make(chan error, len(files))
	var finished atomic.Int32
	var started atomic.Int32
	var wg sync.WaitGroup
	for _, rel := range files {
		rel := rel
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer finished.Add(1)
			path := filepath.Join(fixture.mountpoint, rel)
			f, err := os.Open(path)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = f.Close() }()

			buf := make([]byte, 4096)
			_, err = io.ReadFull(f, buf[:1])
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				errs <- err
				return
			}

			started.Add(1)
			<-hold
			_, err = io.Copy(io.Discard, f)
			if err == nil || errors.Is(err, io.EOF) {
				errs <- nil
				return
			}
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return started.Load() >= 32 || finished.Load() == int32(len(files))
	}, 5*time.Second, 50*time.Millisecond, "concurrent opens never ramped up")
	close(hold)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(stabilityReadTimeout):
		t.Fatal("fd-ulimit readers stayed blocked")
	}

	close(errs)
	for err := range errs {
		if err != nil {
			t.Logf("fd-pressure read returned clean error: %v", err)
		}
	}

	require.True(t, processAlive(cmd), "daemon should survive fd pressure")
	requireFileContentEqual(t,
		filepath.Join(fixture.srcDir, "files/tiny_2b"),
		filepath.Join(fixture.mountpoint, "files/tiny_2b"),
	)
}

func newStabilityFixture(t *testing.T, nydusBin, root string, extra func(*corpus.Corpus)) stabilityFixture {
	t.Helper()

	srcDir := filepath.Join(root, "corpus")
	c := corpus.MakeStandardCorpus(t, srcDir)
	if extra != nil {
		extra(c)
	}

	bootstrap := filepath.Join(root, "stability.bootstrap")
	blobDir := filepath.Join(root, "blobs")
	buildNydusFSImageToDir(t, nydusBin, bootstrap, blobDir, srcDir, 4096)

	return stabilityFixture{
		srcDir:     srcDir,
		bootstrap:  bootstrap,
		blobDir:    blobDir,
		mountpoint: filepath.Join(root, "mnt"),
	}
}

func startStabilityMount(t *testing.T, nydusBin, bootstrap, blobDir, cacheDir, mnt string) (*exec.Cmd, func()) {
	t.Helper()
	prepareMountpoint(t, mnt)

	args := nydusFuseArgs(bootstrap, blobDir, mnt)
	if cacheDir != "" {
		require.NoError(t, os.MkdirAll(cacheDir, 0o755))
		args = append(args, "--cache-dir", cacheDir)
	}

	cmd := exec.Command(nydusBin, args...)
	rawCleanup := startFuseMount(t, cmd, mnt, "nydus stability fuse")
	var once sync.Once
	return cmd, func() {
		once.Do(rawCleanup)
	}
}

func nydusFuseArgs(bootstrap, blobDir, mnt string) []string {
	return []string{"fuse", "--bootstrap", bootstrap, "--blob-dir", blobDir, "--mountpoint", mnt}
}

func prepareMountpoint(t *testing.T, mnt string) {
	t.Helper()
	_ = exec.Command("fusermount", "-u", mnt).Run()
	require.NoError(t, os.MkdirAll(mnt, 0o755))
}

func requireFileContentEqual(t *testing.T, wantPath, gotPath string) {
	t.Helper()
	want, err := os.ReadFile(wantPath)
	require.NoError(t, err)
	got, err := os.ReadFile(gotPath)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	require.NoError(t, err, "invalid %s", name)
	require.Greater(t, v, 0, "%s must be positive", name)
	return v
}

func mountSourceCount(t *testing.T, source string) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/mountinfo")
	require.NoError(t, err)

	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) >= 2 && fields[1] == source {
			count++
		}
	}
	return count
}

func churnReadUntilFailure(path string, seed int, started *atomic.Int32) error {
	rng := mathrand.New(mathrand.NewSource(int64(seed + 1)))
	startedOnce := false
	buf := make([]byte, 32<<10)
	for {
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		size := info.Size()
		if size <= 0 {
			_ = f.Close()
			return fmt.Errorf("%s is unexpectedly empty", path)
		}

		ok := false
		if seed%2 == 0 {
			_, err = io.ReadFull(f, buf)
			ok = err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		} else {
			maxOff := size - int64(len(buf))
			if maxOff < 0 {
				maxOff = 0
			}
			off := int64(0)
			if maxOff > 0 {
				off = rng.Int63n(maxOff + 1)
			}
			var n int
			n, err = f.ReadAt(buf, off)
			ok = err == nil || (errors.Is(err, io.EOF) && n > 0)
		}
		_ = f.Close()

		if ok {
			if !startedOnce {
				started.Add(1)
				startedOnce = true
			}
			continue
		}
		return err
	}
}

func processAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return false
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", cmd.Process.Pid))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	return len(fields) >= 3 && fields[2] != "Z"
}

func statWithin(path string, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		_, err := os.Stat(path)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("stat %s timed out after %s", path, timeout)
	}
}

func tmpfsAvailBytes(t *testing.T, path string) uint64 {
	t.Helper()
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(path, &st))
	return st.Bavail * uint64(st.Bsize)
}
