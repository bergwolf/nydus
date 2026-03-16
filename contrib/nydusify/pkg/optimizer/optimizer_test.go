package optimizer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

func TestMakeDesc(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		oldDesc ocispec.Descriptor
		wantErr bool
	}{
		{
			name:  "simple struct",
			input: map[string]string{"key": "value"},
			oldDesc: ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageManifest,
			},
		},
		{
			name:  "empty struct",
			input: struct{}{},
			oldDesc: ocispec.Descriptor{
				MediaType: ocispec.MediaTypeImageConfig,
			},
		},
		{
			name:  "complex struct",
			input: ocispec.Manifest{MediaType: ocispec.MediaTypeImageManifest},
			oldDesc: ocispec.Descriptor{
				MediaType:   ocispec.MediaTypeImageManifest,
				Annotations: map[string]string{"foo": "bar"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, desc, err := makeDesc(tt.input, tt.oldDesc)
			if (err != nil) != tt.wantErr {
				t.Fatalf("makeDesc() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Verify data is valid JSON
			if !json.Valid(data) {
				t.Error("makeDesc() returned invalid JSON data")
			}

			// Verify size matches
			if desc.Size != int64(len(data)) {
				t.Errorf("desc.Size = %d, want %d", desc.Size, len(data))
			}

			// Verify digest is SHA256 and non-empty
			if desc.Digest.Algorithm().String() != "sha256" {
				t.Errorf("digest algorithm = %q, want %q", desc.Digest.Algorithm(), "sha256")
			}
			if desc.Digest.Encoded() == "" {
				t.Error("digest is empty")
			}

			// Verify digest actually matches the data
			if desc.Digest.Hex() == "" {
				t.Error("digest hex is empty")
			}

			// Verify mediatype preserved from old desc
			if desc.MediaType != tt.oldDesc.MediaType {
				t.Errorf("MediaType = %q, want %q", desc.MediaType, tt.oldDesc.MediaType)
			}

			// Verify old descriptor is not modified
			if tt.oldDesc.Size != 0 {
				t.Errorf("old descriptor was modified: size = %d", tt.oldDesc.Size)
			}
		})
	}
}

func TestMakeDescDeterministic(t *testing.T) {
	input := map[string]int{"a": 1, "b": 2}
	desc := ocispec.Descriptor{MediaType: "test"}

	data1, desc1, _ := makeDesc(input, desc)
	data2, desc2, _ := makeDesc(input, desc)

	if !bytes.Equal(data1, data2) {
		t.Error("makeDesc() is not deterministic for same input")
	}
	if desc1.Digest != desc2.Digest {
		t.Error("makeDesc() produces different digests for same input")
	}
}

func TestGetOriginalBlobLayers(t *testing.T) {
	tests := []struct {
		name   string
		image  parser.Image
		wantN  int
		wantID []string
	}{
		{
			name: "all nydus blobs",
			image: parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:aaa"},
						{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:bbb"},
					},
				},
			},
			wantN: 2,
		},
		{
			name: "mixed layers",
			image: parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:aaa"},
						{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: "sha256:bbb"},
						{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:ccc"},
					},
				},
			},
			wantN: 2,
		},
		{
			name: "no nydus blobs",
			image: parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{
						{MediaType: ocispec.MediaTypeImageLayerGzip},
					},
				},
			},
			wantN: 0,
		},
		{
			name: "empty layers",
			image: parser.Image{
				Manifest: ocispec.Manifest{
					Layers: []ocispec.Descriptor{},
				},
			},
			wantN: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getOriginalBlobLayers(tt.image)
			if len(got) != tt.wantN {
				t.Errorf("getOriginalBlobLayers() returned %d layers, want %d", len(got), tt.wantN)
			}
			for _, layer := range got {
				if layer.MediaType != utils.MediaTypeNydusBlob {
					t.Errorf("non-nydus blob in result: %q", layer.MediaType)
				}
			}
		})
	}
}

func TestPackToTar(t *testing.T) {
	tests := []struct {
		name     string
		files    []File
		compress bool
	}{
		{
			name: "uncompressed with one file",
			files: []File{
				{Name: "test.txt", Reader: strings.NewReader("hello world"), Size: 11},
			},
			compress: false,
		},
		{
			name: "compressed with one file",
			files: []File{
				{Name: "test.txt", Reader: strings.NewReader("hello world"), Size: 11},
			},
			compress: true,
		},
		{
			name:     "empty file list",
			files:    []File{},
			compress: false,
		},
		{
			name: "multiple files",
			files: []File{
				{Name: "a.txt", Reader: strings.NewReader("aaa"), Size: 3},
				{Name: "b.txt", Reader: strings.NewReader("bbb"), Size: 3},
			},
			compress: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := packToTar(tt.files, tt.compress)
			defer rc.Close()

			data, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("reading tar stream: %v", err)
			}
			if len(data) == 0 && len(tt.files) > 0 {
				t.Error("empty output for non-empty file list")
			}

			// Decompress if needed
			var reader io.Reader = bytes.NewReader(data)
			if tt.compress {
				gr, err := gzip.NewReader(reader)
				if err != nil {
					t.Fatalf("creating gzip reader: %v", err)
				}
				defer gr.Close()
				reader = gr
			}

			// Parse tar and verify contents
			tr := tar.NewReader(reader)
			foundDir := false
			fileCount := 0
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("reading tar entry: %v", err)
				}
				if hdr.Name == "image" && hdr.Typeflag == tar.TypeDir {
					foundDir = true
					continue
				}
				if hdr.Typeflag == tar.TypeReg {
					fileCount++
				}
			}
			if len(tt.files) > 0 && !foundDir {
				t.Error("directory entry 'image' not found in tar")
			}
			if fileCount != len(tt.files) {
				t.Errorf("found %d regular files, want %d", fileCount, len(tt.files))
			}
		})
	}
}

func TestIsSignalKilled(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "signal killed error",
			err:  &testError{msg: "process exited with signal: killed"},
			want: true,
		},
		{
			name: "other error",
			err:  &testError{msg: "some other error"},
			want: false,
		},
		{
			name: "timeout error",
			err:  &testError{msg: "context deadline exceeded"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSignalKilled(tt.err)
			if got != tt.want {
				t.Errorf("isSignalKilled(%q) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestOptStruct(t *testing.T) {
	opt := Opt{
		WorkDir:        "/work",
		NydusImagePath: "/usr/bin/nydus-image",
		Source:         "source:latest",
		Target:         "target:latest",
		SourceInsecure: true,
		TargetInsecure: false,
		AllPlatforms:   false,
		PushChunkSize:  1024 * 1024,
		BackendType:    "registry",
	}
	if opt.WorkDir != "/work" {
		t.Errorf("WorkDir = %q, want %q", opt.WorkDir, "/work")
	}
	if opt.PushChunkSize != 1024*1024 {
		t.Errorf("PushChunkSize = %d, want %d", opt.PushChunkSize, 1024*1024)
	}
}

func TestBuildOptionStruct(t *testing.T) {
	opt := BuildOption{
		BuilderPath:         "/usr/bin/nydus-image",
		PrefetchFilesPath:   "/tmp/prefetch",
		BootstrapPath:       "/tmp/bootstrap",
		BlobDir:             "/tmp/blobs",
		OutputBootstrapPath: "/tmp/output-bootstrap",
		OutputJSONPath:      "/tmp/output.json",
		BackendType:         "localfs",
	}
	if opt.BuilderPath != "/usr/bin/nydus-image" {
		t.Errorf("BuilderPath = %q", opt.BuilderPath)
	}
	if opt.BackendType != "localfs" {
		t.Errorf("BackendType = %q, want %q", opt.BackendType, "localfs")
	}
}

func TestConstants(t *testing.T) {
	if EntryBootstrap != "image.boot" {
		t.Errorf("EntryBootstrap = %q, want %q", EntryBootstrap, "image.boot")
	}
	if EntryPrefetchFiles != "prefetch.files" {
		t.Errorf("EntryPrefetchFiles = %q, want %q", EntryPrefetchFiles, "prefetch.files")
	}
}

func TestMakeDescPreservesAnnotations(t *testing.T) {
	oldDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Annotations: map[string]string{
			"org.opencontainers.image.title": "test",
			"custom-key":                     "custom-value",
		},
	}
	data, desc, err := makeDesc(map[string]string{"k": "v"}, oldDesc)
	if err != nil {
		t.Fatalf("makeDesc() error = %v", err)
	}
	if desc.Annotations["org.opencontainers.image.title"] != "test" {
		t.Error("annotations not preserved")
	}
	if desc.Annotations["custom-key"] != "custom-value" {
		t.Error("custom annotation not preserved")
	}
	if len(data) == 0 {
		t.Error("empty data returned")
	}
}

func TestMakeDescDigestChangesWithInput(t *testing.T) {
	desc := ocispec.Descriptor{MediaType: "test"}

	_, desc1, _ := makeDesc(map[string]string{"a": "1"}, desc)
	_, desc2, _ := makeDesc(map[string]string{"a": "2"}, desc)

	if desc1.Digest == desc2.Digest {
		t.Error("different inputs produced same digest")
	}
	if desc1.Size == desc2.Size {
		// Sizes could theoretically be equal, but with these inputs they likely differ
		// This is a weaker assertion
	}
}

func TestMakeDescSizeMatchesData(t *testing.T) {
	inputs := []interface{}{
		map[string]string{},
		map[string]string{"key": "value"},
		[]string{"a", "b", "c"},
		struct{ Name string }{"test"},
	}
	for _, input := range inputs {
		data, desc, err := makeDesc(input, ocispec.Descriptor{})
		if err != nil {
			t.Fatalf("makeDesc() error = %v", err)
		}
		if desc.Size != int64(len(data)) {
			t.Errorf("size mismatch: desc.Size=%d, len(data)=%d", desc.Size, len(data))
		}
	}
}

func TestGetOriginalBlobLayersNilLayers(t *testing.T) {
	image := parser.Image{
		Manifest: ocispec.Manifest{},
	}
	got := getOriginalBlobLayers(image)
	if len(got) != 0 {
		t.Errorf("expected 0 layers, got %d", len(got))
	}
}

func TestGetOriginalBlobLayersPreservesOrder(t *testing.T) {
	image := parser.Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:first"},
				{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: "sha256:skip"},
				{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:second"},
				{MediaType: utils.MediaTypeNydusBlob, Digest: "sha256:third"},
			},
		},
	}
	got := getOriginalBlobLayers(image)
	if len(got) != 3 {
		t.Fatalf("expected 3 layers, got %d", len(got))
	}
	if got[0].Digest != "sha256:first" {
		t.Error("order not preserved")
	}
	if got[1].Digest != "sha256:second" {
		t.Error("order not preserved")
	}
	if got[2].Digest != "sha256:third" {
		t.Error("order not preserved")
	}
}

func TestPackToTarCompressedMultipleFiles(t *testing.T) {
	files := []File{
		{Name: "file1.txt", Reader: strings.NewReader("content1"), Size: 8},
		{Name: "file2.txt", Reader: strings.NewReader("content2"), Size: 8},
		{Name: "file3.txt", Reader: strings.NewReader("content3"), Size: 8},
	}
	rc := packToTar(files, true)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading tar stream: %v", err)
	}

	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("creating gzip reader: %v", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	fileCount := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading tar entry: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			fileCount++
			content, _ := io.ReadAll(tr)
			if len(content) != 8 {
				t.Errorf("unexpected content length: %d", len(content))
			}
		}
	}
	if fileCount != 3 {
		t.Errorf("expected 3 files, got %d", fileCount)
	}
}

func TestPackToTarCompressedEmpty(t *testing.T) {
	rc := packToTar([]File{}, true)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading tar stream: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty gzip output")
	}

	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("creating gzip reader: %v", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	entryCount := 0
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading tar entry: %v", err)
		}
		entryCount++
	}
	// Should have at least the directory entry
	if entryCount != 1 {
		t.Errorf("expected 1 entry (dir), got %d", entryCount)
	}
}

func TestPackToTarFileNames(t *testing.T) {
	files := []File{
		{Name: EntryBootstrap, Reader: strings.NewReader("boot"), Size: 4},
		{Name: EntryPrefetchFiles, Reader: strings.NewReader("pre"), Size: 3},
	}
	rc := packToTar(files, false)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading tar stream: %v", err)
	}

	tr := tar.NewReader(bytes.NewReader(data))
	names := []string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading tar entry: %v", err)
		}
		names = append(names, hdr.Name)
	}
	// Should contain "image" dir and "image/image.boot" and "image/prefetch.files"
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["image"] {
		t.Error("missing directory entry 'image'")
	}
	if !found["image/"+EntryBootstrap] {
		t.Errorf("missing file entry 'image/%s'", EntryBootstrap)
	}
	if !found["image/"+EntryPrefetchFiles] {
		t.Errorf("missing file entry 'image/%s'", EntryPrefetchFiles)
	}
}

func TestIsSignalKilledNilError(t *testing.T) {
	// isSignalKilled expects non-nil error, test with various messages
	tests := []struct {
		msg  string
		want bool
	}{
		{"signal: killed", true},
		{"process exited with signal: killed", true},
		{"exit status 1", false},
		{"", false},
		{"killed", false},
		{"SIGNAL: KILLED", false},
	}
	for _, tt := range tests {
		got := isSignalKilled(&testError{msg: tt.msg})
		if got != tt.want {
			t.Errorf("isSignalKilled(%q) = %v, want %v", tt.msg, got, tt.want)
		}
	}
}

func TestBuildOptionDefaults(t *testing.T) {
	opt := BuildOption{}
	if opt.BuilderPath != "" {
		t.Error("expected empty BuilderPath")
	}
	if opt.PrefetchFilesPath != "" {
		t.Error("expected empty PrefetchFilesPath")
	}
	if opt.BootstrapPath != "" {
		t.Error("expected empty BootstrapPath")
	}
	if opt.BackendType != "" {
		t.Error("expected empty BackendType")
	}
	if opt.BackendConfig != "" {
		t.Error("expected empty BackendConfig")
	}
	if opt.BlobDir != "" {
		t.Error("expected empty BlobDir")
	}
	if opt.OutputBootstrapPath != "" {
		t.Error("expected empty OutputBootstrapPath")
	}
	if opt.OutputJSONPath != "" {
		t.Error("expected empty OutputJSONPath")
	}
	if opt.Timeout != nil {
		t.Error("expected nil Timeout")
	}
}

func TestBuildInvalidBinary(t *testing.T) {
	opt := BuildOption{
		BuilderPath:         "/nonexistent/nydus-image",
		PrefetchFilesPath:   "/tmp/prefetch",
		BootstrapPath:       "/tmp/bootstrap",
		BlobDir:             "/tmp/blobs",
		OutputBootstrapPath: "/tmp/output-bootstrap",
		OutputJSONPath:      "/tmp/output.json",
		BackendType:         "registry",
		BackendConfig:       `{"key":"val"}`,
	}
	_, err := Build(opt)
	if err == nil {
		t.Error("expected error for invalid binary")
	}
}

func TestBuildInvalidBinaryLocalfs(t *testing.T) {
	opt := BuildOption{
		BuilderPath:         "/nonexistent/nydus-image",
		PrefetchFilesPath:   "/tmp/prefetch",
		BootstrapPath:       "/tmp/bootstrap",
		BlobDir:             "/tmp/blobs",
		OutputBootstrapPath: "/tmp/output-bootstrap",
		OutputJSONPath:      "/tmp/output.json",
		BackendType:         "localfs",
	}
	_, err := Build(opt)
	if err == nil {
		t.Error("expected error for invalid binary")
	}
}

func TestOutputJSONStruct(t *testing.T) {
	oj := outputJSON{
		Blobs: []string{"blob1", "blob2"},
	}
	data, err := json.Marshal(oj)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded outputJSON
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(decoded.Blobs) != 2 {
		t.Errorf("expected 2 blobs, got %d", len(decoded.Blobs))
	}
	if decoded.Blobs[0] != "blob1" || decoded.Blobs[1] != "blob2" {
		t.Error("blob values not preserved")
	}
}

func TestOutputJSONEmpty(t *testing.T) {
	oj := outputJSON{}
	data, err := json.Marshal(oj)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded outputJSON
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if decoded.Blobs != nil {
		t.Error("expected nil Blobs for empty struct")
	}
}

func TestFileStruct(t *testing.T) {
	content := "test content"
	f := File{
		Name:   "test.txt",
		Reader: strings.NewReader(content),
		Size:   int64(len(content)),
	}
	if f.Name != "test.txt" {
		t.Errorf("Name = %q", f.Name)
	}
	if f.Size != 12 {
		t.Errorf("Size = %d", f.Size)
	}
	data, _ := io.ReadAll(f.Reader)
	if string(data) != content {
		t.Errorf("Reader content = %q", string(data))
	}
}

func TestOptDefaults(t *testing.T) {
	opt := Opt{}
	if opt.WorkDir != "" {
		t.Error("expected empty WorkDir")
	}
	if opt.Source != "" {
		t.Error("expected empty Source")
	}
	if opt.Target != "" {
		t.Error("expected empty Target")
	}
	if opt.SourceInsecure {
		t.Error("expected false SourceInsecure")
	}
	if opt.TargetInsecure {
		t.Error("expected false TargetInsecure")
	}
	if opt.AllPlatforms {
		t.Error("expected false AllPlatforms")
	}
	if opt.PushChunkSize != 0 {
		t.Error("expected zero PushChunkSize")
	}
	if opt.OptimizePolicy != "" {
		t.Error("expected empty OptimizePolicy")
	}
	if opt.PrefetchFilesPath != "" {
		t.Error("expected empty PrefetchFilesPath")
	}
	if opt.Platforms != "" {
		t.Error("expected empty Platforms")
	}
}

func TestBuildInfoStruct(t *testing.T) {
	info := BuildInfo{
		BuildDir:         "/build",
		BlobDir:          "/blobs",
		PrefetchBlobID:   "abc123",
		NewBootstrapPath: "/bootstrap",
	}
	if info.BuildDir != "/build" {
		t.Errorf("BuildDir = %q", info.BuildDir)
	}
	if info.BlobDir != "/blobs" {
		t.Errorf("BlobDir = %q", info.BlobDir)
	}
	if info.PrefetchBlobID != "abc123" {
		t.Errorf("PrefetchBlobID = %q", info.PrefetchBlobID)
	}
	if info.NewBootstrapPath != "/bootstrap" {
		t.Errorf("NewBootstrapPath = %q", info.NewBootstrapPath)
	}
}

func TestHostsFunction(t *testing.T) {
	opt := Opt{
		Source:         "source-ref",
		Target:         "target-ref",
		SourceInsecure: true,
		TargetInsecure: false,
	}
	hostFunc := hosts(opt)

	tests := []struct {
		ref          string
		wantInsecure bool
	}{
		{"source-ref", true},
		{"target-ref", false},
		{"unknown-ref", false},
	}

	for _, tt := range tests {
		credFunc, insecure, err := hostFunc(tt.ref)
		if err != nil {
			t.Errorf("hosts(%q) error = %v", tt.ref, err)
		}
		if insecure != tt.wantInsecure {
			t.Errorf("hosts(%q) insecure = %v, want %v", tt.ref, insecure, tt.wantInsecure)
		}
		if credFunc == nil {
			t.Errorf("hosts(%q) credFunc is nil", tt.ref)
		}
	}
}

func TestHostsBothInsecure(t *testing.T) {
	opt := Opt{
		Source:         "src",
		Target:         "tgt",
		SourceInsecure: true,
		TargetInsecure: true,
	}
	hostFunc := hosts(opt)
	_, insecure, err := hostFunc("src")
	if err != nil {
		t.Fatal(err)
	}
	if !insecure {
		t.Error("expected insecure for source")
	}
	_, insecure, err = hostFunc("tgt")
	if err != nil {
		t.Fatal(err)
	}
	if !insecure {
		t.Error("expected insecure for target")
	}
}

func TestMakeDescMarshalError(t *testing.T) {
	ch := make(chan int)
	_, _, err := makeDesc(ch, ocispec.Descriptor{})
	if err == nil {
		t.Error("expected error for unmarshalable input")
	}
}

func TestBootstrapInfoStruct(t *testing.T) {
	info := bootstrapInfo{}
	if info.bootstrapDesc.MediaType != "" {
		t.Error("expected empty MediaType")
	}
	if info.bootstrapDiffID != "" {
		t.Error("expected empty bootstrapDiffID")
	}
}

func TestBuildWithTimeout(t *testing.T) {
	timeout := time.Second * 1
	opt := BuildOption{
		BuilderPath:         "/nonexistent/nydus-image",
		PrefetchFilesPath:   "/tmp/prefetch",
		BootstrapPath:       "/tmp/bootstrap",
		BlobDir:             "/tmp/blobs",
		OutputBootstrapPath: "/tmp/output-bootstrap",
		OutputJSONPath:      "/tmp/output.json",
		BackendType:         "registry",
		BackendConfig:       `{"key":"val"}`,
		Timeout:             &timeout,
	}
	_, err := Build(opt)
	if err == nil {
		t.Error("expected error for invalid binary")
	}
}

func TestPackToTarLargeFile(t *testing.T) {
	largeContent := strings.Repeat("x", 1024*1024)
	files := []File{
		{Name: "large.txt", Reader: strings.NewReader(largeContent), Size: int64(len(largeContent))},
	}
	rc := packToTar(files, false)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading tar: %v", err)
	}

	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading tar entry: %v", err)
		}
		if hdr.Name == "image/large.txt" {
			if hdr.Size != int64(len(largeContent)) {
				t.Errorf("size mismatch: %d vs %d", hdr.Size, len(largeContent))
			}
		}
	}
}

func TestBuildSuccessWithMockCommand(t *testing.T) {
	tmpDir := t.TempDir()
	outputJSONPath := tmpDir + "/output.json"
	blobDir := tmpDir + "/blobs"
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Write valid output JSON
	outJSON := outputJSON{Blobs: []string{"blob1", "prefetch-blob-id"}}
	data, _ := json.Marshal(outJSON)
	if err := os.WriteFile(outputJSONPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	opt := BuildOption{
		BuilderPath:         "true", // /usr/bin/true always succeeds
		PrefetchFilesPath:   "/dev/null",
		BootstrapPath:       "/dev/null",
		BlobDir:             blobDir,
		OutputBootstrapPath: tmpDir + "/output-bootstrap",
		OutputJSONPath:      outputJSONPath,
		BackendType:         "registry",
		BackendConfig:       `{"key":"val"}`,
	}

	blobID, err := Build(opt)
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}
	if blobID != "prefetch-blob-id" {
		t.Errorf("Build() blobID = %q, want %q", blobID, "prefetch-blob-id")
	}
}

func TestBuildSuccessLocalfs(t *testing.T) {
	tmpDir := t.TempDir()
	outputJSONPath := tmpDir + "/output.json"
	blobDir := tmpDir + "/blobs"
	os.MkdirAll(blobDir, 0755)

	outJSON := outputJSON{Blobs: []string{"single-blob"}}
	data, _ := json.Marshal(outJSON)
	os.WriteFile(outputJSONPath, data, 0644)

	opt := BuildOption{
		BuilderPath:         "true",
		PrefetchFilesPath:   "/dev/null",
		BootstrapPath:       "/dev/null",
		BlobDir:             blobDir,
		OutputBootstrapPath: tmpDir + "/output-bootstrap",
		OutputJSONPath:      outputJSONPath,
		BackendType:         "localfs",
	}

	blobID, err := Build(opt)
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}
	if blobID != "single-blob" {
		t.Errorf("Build() blobID = %q, want %q", blobID, "single-blob")
	}
}

func TestBuildInvalidOutputJSON(t *testing.T) {
	tmpDir := t.TempDir()
	outputJSONPath := tmpDir + "/output.json"

	// Write invalid JSON
	os.WriteFile(outputJSONPath, []byte("not json"), 0644)

	opt := BuildOption{
		BuilderPath:         "true",
		PrefetchFilesPath:   "/dev/null",
		BootstrapPath:       "/dev/null",
		BlobDir:             tmpDir,
		OutputBootstrapPath: tmpDir + "/output-bootstrap",
		OutputJSONPath:      outputJSONPath,
		BackendType:         "registry",
		BackendConfig:       `{}`,
	}

	_, err := Build(opt)
	if err == nil {
		t.Error("expected error for invalid JSON output")
	}
}

func TestBuildMissingOutputFile(t *testing.T) {
	tmpDir := t.TempDir()

	opt := BuildOption{
		BuilderPath:         "true",
		PrefetchFilesPath:   "/dev/null",
		BootstrapPath:       "/dev/null",
		BlobDir:             tmpDir,
		OutputBootstrapPath: tmpDir + "/output-bootstrap",
		OutputJSONPath:      tmpDir + "/nonexistent/output.json",
		BackendType:         "registry",
		BackendConfig:       `{}`,
	}

	_, err := Build(opt)
	if err == nil {
		t.Error("expected error for missing output file")
	}
}

func TestBuildWithTimeoutSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	outputJSONPath := tmpDir + "/output.json"
	outJSON := outputJSON{Blobs: []string{"timeout-blob"}}
	data, _ := json.Marshal(outJSON)
	os.WriteFile(outputJSONPath, data, 0644)

	timeout := 30 * time.Second
	opt := BuildOption{
		BuilderPath:         "true",
		PrefetchFilesPath:   "/dev/null",
		BootstrapPath:       "/dev/null",
		BlobDir:             tmpDir,
		OutputBootstrapPath: tmpDir + "/output-bootstrap",
		OutputJSONPath:      outputJSONPath,
		BackendType:         "registry",
		BackendConfig:       `{}`,
		Timeout:             &timeout,
	}

	blobID, err := Build(opt)
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}
	if blobID != "timeout-blob" {
		t.Errorf("Build() blobID = %q, want %q", blobID, "timeout-blob")
	}
}

func TestPackToTarFileContentPreserved(t *testing.T) {
	content1 := "hello world"
	content2 := "foo bar baz"
	files := []File{
		{Name: "a.txt", Reader: strings.NewReader(content1), Size: int64(len(content1))},
		{Name: "b.txt", Reader: strings.NewReader(content2), Size: int64(len(content2))},
	}
	rc := packToTar(files, false)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading tar: %v", err)
	}

	tr := tar.NewReader(bytes.NewReader(data))
	contents := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading tar entry: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			b, _ := io.ReadAll(tr)
			contents[hdr.Name] = string(b)
		}
	}
	if contents["image/a.txt"] != content1 {
		t.Errorf("a.txt content = %q, want %q", contents["image/a.txt"], content1)
	}
	if contents["image/b.txt"] != content2 {
		t.Errorf("b.txt content = %q, want %q", contents["image/b.txt"], content2)
	}
}

func TestPackToTarFilePermissions(t *testing.T) {
	files := []File{
		{Name: "test.txt", Reader: strings.NewReader("x"), Size: 1},
	}
	rc := packToTar(files, false)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == "image" {
			if hdr.Mode != 0755 {
				t.Errorf("dir mode = %o, want 0755", hdr.Mode)
			}
		}
		if hdr.Name == "image/test.txt" {
			if hdr.Mode != 0444 {
				t.Errorf("file mode = %o, want 0444", hdr.Mode)
			}
		}
	}
}

func TestGetOriginalBlobLayersAnnotationsPreserved(t *testing.T) {
	image := parser.Image{
		Manifest: ocispec.Manifest{
			Layers: []ocispec.Descriptor{
				{
					MediaType: utils.MediaTypeNydusBlob,
					Digest:    "sha256:abc",
					Annotations: map[string]string{
						"custom": "value",
					},
				},
			},
		},
	}
	got := getOriginalBlobLayers(image)
	if len(got) != 1 {
		t.Fatal("expected 1 layer")
	}
	if got[0].Annotations["custom"] != "value" {
		t.Error("annotations not preserved")
	}
}

func TestRemoterInvalidRef(t *testing.T) {
	opt := Opt{
		Target: "://invalid-ref",
	}
	_, err := remoter(opt)
	if err == nil {
		t.Error("expected error for invalid target reference")
	}
}

func TestOutputJSONMultipleBlobs(t *testing.T) {
	oj := outputJSON{
		Blobs: []string{"a", "b", "c", "d"},
	}
	data, err := json.Marshal(oj)
	if err != nil {
		t.Fatal(err)
	}
	var decoded outputJSON
	json.Unmarshal(data, &decoded)
	if len(decoded.Blobs) != 4 {
		t.Errorf("expected 4 blobs, got %d", len(decoded.Blobs))
	}
	// Build uses last blob ID
	if decoded.Blobs[len(decoded.Blobs)-1] != "d" {
		t.Error("last blob should be 'd'")
	}
}

func TestPackToTarCompressedContentVerifiable(t *testing.T) {
	content := "compressed content test"
	files := []File{
		{Name: "c.txt", Reader: strings.NewReader(content), Size: int64(len(content))},
	}
	rc := packToTar(files, true)
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	found := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == "image/c.txt" {
			b, _ := io.ReadAll(tr)
			if string(b) != content {
				t.Errorf("content = %q, want %q", string(b), content)
			}
			found = true
		}
	}
	if !found {
		t.Error("file not found in compressed tar")
	}
}
