package e2e

import (
	"archive/tar"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dragonflyoss/nydus/tests/e2e/corpus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The cross-version compatibility check builds images with one nydus binary
// and checks and reads them with another, so a change to the on-disk format
// that a released binary cannot read (or that cannot read what a released
// binary wrote) fails loudly. Both default to the in-tree release build; CI
// points one of them at the last released version in each direction.
const (
	compatBuilderEnv = "NYDUS_COMPAT_BUILDER"
	compatReaderEnv  = "NYDUS_COMPAT_READER"
)

// compatLayout is one `nydus build` configuration exercised by the check.
type compatLayout struct {
	name string
	// native layouts (erofs-*) carry no blob meta and need no cache to mount.
	native bool
	args   []string
}

var compatLayouts = []compatLayout{
	{name: "zstd", args: []string{"--compressor", "zstd"}},
	{name: "zstd-4k", args: []string{"--compressor", "zstd", "--chunk-size", "4096"}},
	{name: "zstd-4k-group-64k", args: []string{"--compressor", "zstd", "--chunk-size", "4096", "--chunk-group-minimum-size", "65536"}},
	{name: "zstd-no-digest", args: []string{"--compressor", "zstd", "--digester", "none"}},
	{name: "lz4-64k", args: []string{"--compressor", "lz4", "--chunk-size", "65536"}},
	{name: "none-4k", args: []string{"--compressor", "none", "--chunk-size", "4096"}},
	{name: "erofs-none", native: true, args: []string{"--compressor", "erofs-none"}},
	{name: "erofs-lz4", native: true, args: []string{"--compressor", "erofs-lz4"}},
	{name: "erofs-zstd", native: true, args: []string{"--compressor", "erofs-zstd"}},
}

// compatMergeLayouts are the layer layouts merged into an overlaid bootstrap.
var compatMergeLayouts = []compatLayout{
	{name: "zstd-4k", args: []string{"--compressor", "zstd", "--chunk-size", "4096"}},
	{name: "erofs-lz4", native: true, args: []string{"--compressor", "erofs-lz4"}},
}

func TestCrossVersionCompatibility(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}

	builder := lookupBinFromEnv(t, compatBuilderEnv, "nydus")
	reader := lookupBinFromEnv(t, compatReaderEnv, "nydus")
	t.Logf("builder: %s (%s)", builder, compatVersion(t, builder))
	t.Logf("reader:  %s (%s)", reader, compatVersion(t, reader))

	root := t.TempDir()
	src := filepath.Join(root, "corpus")
	c := corpus.MakeStandardCorpus(t, src)
	c.CreateUnixSocket(t, "special/socket")
	// Files spanning several chunks at the default 2MiB chunk size, one of
	// them duplicated so chunk dedup lands in the image.
	large := make([]byte, 5<<20+123)
	_, err := rand.Read(large)
	require.NoError(t, err)
	c.CreateFile(t, "compat/large", large)
	c.CreateFile(t, "compat/large_dup", large)
	c.CreateSparseFile(t, "compat/sparse_large", 8<<20+1, map[int64][]byte{
		0:       []byte("HEAD"),
		3 << 20: []byte("MID"),
		8 << 20: []byte("Z"),
	})

	for _, layout := range compatLayouts {
		layout := layout
		t.Run(layout.name, func(t *testing.T) {
			dir := filepath.Join(root, layout.name)
			blobDir := filepath.Join(dir, "blobs")
			bootstrap := filepath.Join(dir, "image.bootstrap")
			mnt := filepath.Join(dir, "mnt")
			blob := compatBuild(t, builder, blobDir, bootstrap, src, layout)

			t.Run("Check", func(t *testing.T) {
				compatRun(t, reader, "check", "--blob", blob)
				compatRun(t, reader, "check", "--bootstrap", bootstrap, "--blob-dir", blobDir)
			})

			t.Run("FuseBlob", func(t *testing.T) {
				unmount := mountNydus(t, reader, "", blob, mnt)
				defer unmount()
				compatVerifyTree(t, src, mnt)
			})

			t.Run("FuseBootstrap", func(t *testing.T) {
				unmount := mountNydusBootstrap(t, reader, bootstrap, blobDir, mnt)
				defer unmount()
				compatVerifyTree(t, src, mnt)
			})

			if !layout.native {
				t.Run("FuseBootstrapCache", func(t *testing.T) {
					cacheDir := filepath.Join(dir, "cache")
					unmount := mountNydusBootstrapWithCache(t, reader, bootstrap, blobDir, cacheDir, mnt)
					defer unmount()
					compatVerifyTree(t, src, mnt)
				})
			}

			t.Run("Export", func(t *testing.T) {
				output := filepath.Join(dir, "layer.tar")
				compatRun(t, reader, "export", blob, "--output", output)
				compatVerifyExport(t, src, output)
			})
		})
	}

	for _, layout := range compatMergeLayouts {
		layout := layout
		t.Run("merged-"+layout.name, func(t *testing.T) {
			dir := filepath.Join(root, "merged-"+layout.name)
			blobDir := filepath.Join(dir, "blobs")
			expectedDir := filepath.Join(dir, "expected")
			merged := filepath.Join(dir, "merged.bootstrap")
			mnt := filepath.Join(dir, "mnt")

			layerDirs := []string{
				filepath.Join(dir, "layer1"),
				filepath.Join(dir, "layer2"),
				filepath.Join(dir, "layer3"),
			}
			prepareMergedE2ECorpora(t, layerDirs[0], layerDirs[1], layerDirs[2], expectedDir)
			var blobs []string
			for _, layerDir := range layerDirs {
				bootstrap := layerDir + ".bootstrap"
				blobs = append(blobs, compatBuild(t, builder, blobDir, bootstrap, layerDir, layout))
			}
			mergeNydusBootstrap(t, builder, merged, blobs...)

			t.Run("Check", func(t *testing.T) {
				compatRun(t, reader, "check", "--bootstrap", merged, "--blob-dir", blobDir)
			})

			t.Run("FuseBootstrap", func(t *testing.T) {
				unmount := mountNydusBootstrap(t, reader, merged, blobDir, mnt)
				defer unmount()
				compatVerifyTree(t, expectedDir, mnt)
				verifyWhiteoutResults(t, mnt)
			})

			if !layout.native {
				t.Run("FuseBootstrapCache", func(t *testing.T) {
					cacheDir := filepath.Join(dir, "cache")
					unmount := mountNydusBootstrapWithCache(t, reader, merged, blobDir, cacheDir, mnt)
					defer unmount()
					compatVerifyTree(t, expectedDir, mnt)
					verifyWhiteoutResults(t, mnt)
				})
			}
		})
	}
}

// compatVersion returns the `nydus --version` line of bin.
func compatVersion(t *testing.T, bin string) string {
	t.Helper()
	out, err := exec.Command(bin, "--version").CombinedOutput()
	require.NoError(t, err, "%s --version: %s", bin, out)
	return strings.TrimSpace(string(out))
}

// compatRun runs bin with args, failing the test with its output on error.
func compatRun(t *testing.T, bin string, args ...string) {
	t.Helper()
	out, err := exec.Command(bin, args...).CombinedOutput()
	require.NoError(t, err, "nydus %s failed: %s", strings.Join(args, " "), out)
	t.Logf("nydus %s:\n%s", strings.Join(args, " "), out)
}

// compatBuild builds src into blobDir with bootstrap alongside and returns the
// path of the single new full blob, which must be named by its SHA256.
func compatBuild(t *testing.T, bin, blobDir, bootstrap, src string, layout compatLayout) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(blobDir, 0755))
	before := listFilesInDir(t, blobDir)

	args := append([]string{"build", "--blob-dir", blobDir, "--bootstrap", bootstrap}, layout.args...)
	compatRun(t, bin, append(args, src)...)

	var blobs, metas []string
	for path := range listFilesInDir(t, blobDir) {
		if _, existed := before[path]; existed {
			continue
		}
		switch base := filepath.Base(path); {
		case sha256FilenamePattern.MatchString(base):
			blobs = append(blobs, path)
		case blobMetaFilenamePattern.MatchString(base):
			metas = append(metas, path)
		default:
			require.Failf(t, "unexpected build output", "%s", path)
		}
	}
	require.Len(t, blobs, 1, "expected exactly one new blob in %s", blobDir)
	require.Equal(t, filepath.Base(blobs[0]), sha256File(t, blobs[0]), "blob must be named by its SHA256")
	if layout.native {
		require.Empty(t, metas, "native layers carry no blob meta")
	} else {
		require.Equal(t, []string{blobs[0] + ".blob.meta"}, metas)
	}
	return blobs[0]
}

// compatVerifyTree diffs the mount against src and, beyond the sampled
// content comparison of roDiffTree, hashes every regular file in full.
func compatVerifyTree(t *testing.T, src, mnt string) {
	t.Helper()
	roDiffTree(t, src, mnt, true)
	t.Run("FullContent", func(t *testing.T) {
		roWalkPairs(t, src, mnt, func(rel, srcPath, mntPath string) {
			var st unix.Stat_t
			require.NoError(t, unix.Lstat(srcPath, &st))
			if st.Mode&unix.S_IFMT == unix.S_IFREG {
				require.Equal(t, sha256File(t, srcPath), sha256File(t, mntPath), "%s: content", rel)
			}
		})
	})
}

// compatVerifyExport checks that the exported OCI layer tar reproduces src:
// the same paths (sockets aside, which tar cannot carry), types, modes,
// owners, mtimes, device numbers, link targets, hardlink groups, xattrs and
// file content.
func compatVerifyExport(t *testing.T, src, tarPath string) {
	t.Helper()
	f, err := os.Open(tarPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var got []string
	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rel := strings.TrimSuffix(hdr.Name, "/")
		got = append(got, rel)
		srcPath := filepath.Join(src, rel)

		var st unix.Stat_t
		require.NoError(t, unix.Lstat(srcPath, &st), "%s: not in the source", rel)
		require.Equal(t, int64(st.Mode&07777), hdr.Mode&07777, "%s: mode", rel)
		require.Equal(t, int(st.Uid), hdr.Uid, "%s: uid", rel)
		require.Equal(t, int(st.Gid), hdr.Gid, "%s: gid", rel)
		require.Equal(t, st.Mtim.Sec, hdr.ModTime.Unix(), "%s: mtime", rel)
		require.Equal(t, st.Mtim.Nsec, int64(hdr.ModTime.Nanosecond()), "%s: mtime nsec", rel)

		kind := st.Mode & unix.S_IFMT
		switch hdr.Typeflag {
		case tar.TypeDir:
			require.Equal(t, uint32(unix.S_IFDIR), kind, "%s: type", rel)
		case tar.TypeReg:
			require.Equal(t, uint32(unix.S_IFREG), kind, "%s: type", rel)
			h := sha256.New()
			_, err := io.Copy(h, tr)
			require.NoError(t, err)
			require.Equal(t, sha256File(t, srcPath), hex.EncodeToString(h.Sum(nil)), "%s: content", rel)
		case tar.TypeLink:
			require.Equal(t, uint32(unix.S_IFREG), kind, "%s: type", rel)
			var target unix.Stat_t
			require.NoError(t, unix.Lstat(filepath.Join(src, hdr.Linkname), &target))
			require.Equal(t, st.Ino, target.Ino, "%s: hardlink to %s", rel, hdr.Linkname)
		case tar.TypeSymlink:
			require.Equal(t, uint32(unix.S_IFLNK), kind, "%s: type", rel)
			want, err := os.Readlink(srcPath)
			require.NoError(t, err)
			require.Equal(t, want, hdr.Linkname, "%s: symlink target", rel)
		case tar.TypeChar, tar.TypeBlock:
			want := uint32(unix.S_IFCHR)
			if hdr.Typeflag == tar.TypeBlock {
				want = unix.S_IFBLK
			}
			require.Equal(t, want, kind, "%s: type", rel)
			require.Equal(t, int64(unix.Major(st.Rdev)), hdr.Devmajor, "%s: major", rel)
			require.Equal(t, int64(unix.Minor(st.Rdev)), hdr.Devminor, "%s: minor", rel)
		case tar.TypeFifo:
			require.Equal(t, uint32(unix.S_IFIFO), kind, "%s: type", rel)
		default:
			require.Failf(t, "unexpected tar entry type", "%s: %c", rel, hdr.Typeflag)
		}

		if kind != unix.S_IFLNK {
			want := map[string]string{}
			for _, name := range roVisibleXattrs(t, srcPath) {
				want[name] = string(roGetXattr(t, srcPath, name))
			}
			gotXattrs := map[string]string{}
			for key, value := range hdr.PAXRecords {
				if name, ok := strings.CutPrefix(key, "SCHILY.xattr."); ok && !strings.HasPrefix(name, "trusted.nydus.") {
					gotXattrs[name] = value
				}
			}
			require.Equal(t, want, gotXattrs, "%s: xattrs", rel)
		}
	}

	var want []string
	for _, rel := range roRelativePaths(t, src) {
		var st unix.Stat_t
		require.NoError(t, unix.Lstat(filepath.Join(src, rel), &st))
		if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			want = append(want, rel)
		}
	}
	sort.Strings(got)
	require.Equal(t, want, got, "exported paths")
}
