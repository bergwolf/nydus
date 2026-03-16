package tool

import (
	"encoding/json"
	"testing"
)

func TestBlobInfoString(t *testing.T) {
	info := &BlobInfo{
		BlobID:           "abc123",
		CompressedSize:   1024,
		DecompressedSize: 2048,
		ReadaheadOffset:  0,
		ReadaheadSize:    512,
	}
	s := info.String()
	if s == "" {
		t.Fatal("BlobInfo.String() returned empty string")
	}

	// Verify it produces valid JSON
	var parsed BlobInfo
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("BlobInfo.String() produced invalid JSON: %v", err)
	}
	if parsed.BlobID != "abc123" {
		t.Errorf("BlobID = %q, want %q", parsed.BlobID, "abc123")
	}
	if parsed.CompressedSize != 1024 {
		t.Errorf("CompressedSize = %d, want 1024", parsed.CompressedSize)
	}
	if parsed.DecompressedSize != 2048 {
		t.Errorf("DecompressedSize = %d, want 2048", parsed.DecompressedSize)
	}
	if parsed.ReadaheadSize != 512 {
		t.Errorf("ReadaheadSize = %d, want 512", parsed.ReadaheadSize)
	}
}

func TestBlobInfoStringZeroValues(t *testing.T) {
	info := &BlobInfo{}
	s := info.String()
	var parsed BlobInfo
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		t.Fatalf("BlobInfo.String() with zero values produced invalid JSON: %v", err)
	}
	if parsed.BlobID != "" {
		t.Errorf("BlobID = %q, want empty", parsed.BlobID)
	}
}

func TestBlobInfoListString(t *testing.T) {
	tests := []struct {
		name  string
		infos BlobInfoList
	}{
		{
			name:  "empty list",
			infos: BlobInfoList{},
		},
		{
			name: "single item",
			infos: BlobInfoList{
				{BlobID: "blob1", CompressedSize: 100},
			},
		},
		{
			name: "multiple items",
			infos: BlobInfoList{
				{BlobID: "blob1", CompressedSize: 100},
				{BlobID: "blob2", CompressedSize: 200},
				{BlobID: "blob3", CompressedSize: 300},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.infos.String()
			if s == "" {
				t.Fatal("BlobInfoList.String() returned empty string")
			}
			var parsed BlobInfoList
			if err := json.Unmarshal([]byte(s), &parsed); err != nil {
				t.Fatalf("BlobInfoList.String() produced invalid JSON: %v", err)
			}
			if len(parsed) != len(tt.infos) {
				t.Errorf("parsed length = %d, want %d", len(parsed), len(tt.infos))
			}
			for i := range tt.infos {
				if parsed[i].BlobID != tt.infos[i].BlobID {
					t.Errorf("parsed[%d].BlobID = %q, want %q", i, parsed[i].BlobID, tt.infos[i].BlobID)
				}
			}
		})
	}
}

func TestNewInspector(t *testing.T) {
	inspector := NewInspector("/usr/bin/nydus-image")
	if inspector == nil {
		t.Fatal("NewInspector() returned nil")
	}
	if inspector.binaryPath != "/usr/bin/nydus-image" {
		t.Errorf("binaryPath = %q, want %q", inspector.binaryPath, "/usr/bin/nydus-image")
	}
}

func TestInspectUnsupportedOperation(t *testing.T) {
	inspector := NewInspector("/nonexistent")
	_, err := inspector.Inspect(InspectOption{
		Operation: 999, // unsupported
		Bootstrap: "/some/path",
	})
	if err == nil {
		t.Error("Inspect() with unsupported operation should return error")
	}
}

func TestInspectGetBlobsInvalidBinary(t *testing.T) {
	inspector := NewInspector("/nonexistent/binary")
	_, err := inspector.Inspect(InspectOption{
		Operation: GetBlobs,
		Bootstrap: "/some/path",
	})
	if err == nil {
		t.Error("Inspect() with invalid binary should return error")
	}
}

func TestGetBlobsConstant(t *testing.T) {
	if GetBlobs != 0 {
		t.Errorf("GetBlobs = %d, want 0", GetBlobs)
	}
}

func TestNewInspectorEmpty(t *testing.T) {
	inspector := NewInspector("")
	if inspector == nil {
		t.Fatal("NewInspector() returned nil")
	}
	if inspector.binaryPath != "" {
		t.Errorf("binaryPath = %q, want empty", inspector.binaryPath)
	}
}

func TestBlobInfoFields(t *testing.T) {
	info := BlobInfo{
		BlobID:           "test-id",
		CompressedSize:   1000,
		DecompressedSize: 2000,
		ReadaheadOffset:  100,
		ReadaheadSize:    200,
	}
	if info.BlobID != "test-id" {
		t.Errorf("BlobID = %q", info.BlobID)
	}
	if info.CompressedSize != 1000 {
		t.Errorf("CompressedSize = %d", info.CompressedSize)
	}
	if info.DecompressedSize != 2000 {
		t.Errorf("DecompressedSize = %d", info.DecompressedSize)
	}
	if info.ReadaheadOffset != 100 {
		t.Errorf("ReadaheadOffset = %d", info.ReadaheadOffset)
	}
	if info.ReadaheadSize != 200 {
		t.Errorf("ReadaheadSize = %d", info.ReadaheadSize)
	}
}

func TestBlobInfoListNil(t *testing.T) {
	var list BlobInfoList
	s := list.String()
	if s != "null" {
		t.Errorf("nil BlobInfoList.String() = %q, want %q", s, "null")
	}
}
