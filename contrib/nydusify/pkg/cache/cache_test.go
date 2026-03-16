// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"

	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/backend"
	"github.com/dragonflyoss/nydus/contrib/nydusify/pkg/utils"
)

func makeRecord(id int64, hashBlob bool) *Record {
	var blobDesc *ocispec.Descriptor
	idStr := strconv.FormatInt(id, 10)
	if hashBlob {
		blobDesc = &ocispec.Descriptor{
			MediaType: utils.MediaTypeNydusBlob,
			Digest:    digest.FromString("blob-" + idStr),
			Size:      id,
		}
	}
	return &Record{
		SourceChainID: digest.FromString("chain-" + idStr),
		NydusBootstrapDesc: &ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayerGzip,
			Digest:    digest.FromString("bootstrap-" + idStr),
			Size:      id,
		},
		NydusBootstrapDiffID: digest.FromString("bootstrap-uncompressed-" + idStr),
		NydusBlobDesc:        blobDesc,
	}
}

func makeBootstrapLayer(id int64, hasBlob bool) ocispec.Descriptor {
	idStr := strconv.FormatInt(id, 10)
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromString("bootstrap-" + idStr),
		Size:      id,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusBootstrap:     "true",
			utils.LayerAnnotationNydusFsVersion:     "6",
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain-" + idStr).String(),
			utils.LayerAnnotationUncompressed:       digest.FromString("bootstrap-uncompressed-" + idStr).String(),
		},
	}
	if hasBlob {
		desc.Annotations[utils.LayerAnnotationNydusBlobDigest] = digest.FromString("blob-" + idStr).String()
		desc.Annotations[utils.LayerAnnotationNydusBlobSize] = fmt.Sprintf("%d", id)
	}
	return desc
}

func makeBlobLayer(id int64) ocispec.Descriptor {
	idStr := strconv.FormatInt(id, 10)
	return ocispec.Descriptor{
		MediaType: utils.MediaTypeNydusBlob,
		Digest:    digest.FromString("blob-" + idStr),
		Size:      id,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusBlob:          "true",
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain-" + idStr).String(),
		},
	}
}

func testWithBackend(t *testing.T, _backend backend.Backend) {
	cache, err := New(nil, Opt{
		MaxRecords:     3,
		DockerV2Format: false,
		Backend:        _backend,
		FsVersion:      "6",
	})
	assert.Nil(t, err)

	exported := []*Record{
		makeRecord(1, true),
		makeRecord(2, true),
		makeRecord(3, false),
	}
	cache.Record(exported)
	cache.Record(exported)
	layers := cache.exportRecordsToLayers()

	if _backend.Type() == backend.RegistryBackend {
		assert.Equal(t, layers, []ocispec.Descriptor{
			makeBootstrapLayer(1, false),
			makeBlobLayer(1),
			makeBootstrapLayer(2, false),
			makeBlobLayer(2),
			makeBootstrapLayer(3, false),
		})
	} else {
		assert.Equal(t, layers, []ocispec.Descriptor{
			makeBootstrapLayer(1, true),
			makeBootstrapLayer(2, true),
			makeBootstrapLayer(3, false),
		})
	}

	cache.importRecordsFromLayers(layers)
	cache.Record([]*Record{
		makeRecord(4, true),
		makeRecord(5, true),
	})
	layers = cache.exportRecordsToLayers()

	if _backend.Type() == backend.RegistryBackend {
		assert.Equal(t, layers, []ocispec.Descriptor{
			makeBootstrapLayer(4, false),
			makeBlobLayer(4),
			makeBootstrapLayer(5, false),
			makeBlobLayer(5),
			makeBootstrapLayer(1, false),
			makeBlobLayer(1),
		})
	} else {
		assert.Equal(t, layers, []ocispec.Descriptor{
			makeBootstrapLayer(4, true),
			makeBootstrapLayer(5, true),
			makeBootstrapLayer(1, true),
		})
	}
}

func TestCache(t *testing.T) {
	testWithBackend(t, &backend.Registry{})
	testWithBackend(t, &backend.OSSBackend{})
}

func TestGetReferenceBlobsValidJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonStr  string
		expected []string
	}{
		{
			name:     "multiple blobs",
			jsonStr:  `["blob1","blob2","blob3"]`,
			expected: []string{"blob1", "blob2", "blob3"},
		},
		{
			name:     "single blob",
			jsonStr:  `["blob1"]`,
			expected: []string{"blob1"},
		},
		{
			name:     "empty array",
			jsonStr:  `[]`,
			expected: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &Record{
				NydusBootstrapDesc: &ocispec.Descriptor{
					Annotations: map[string]string{
						utils.LayerAnnotationNydusReferenceBlobIDs: tt.jsonStr,
					},
				},
			}
			result := record.GetReferenceBlobs()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetReferenceBlobsEmptyAnnotation(t *testing.T) {
	record := &Record{
		NydusBootstrapDesc: &ocispec.Descriptor{
			Annotations: map[string]string{
				utils.LayerAnnotationNydusReferenceBlobIDs: "",
			},
		},
	}
	result := record.GetReferenceBlobs()
	assert.Equal(t, []string{}, result)
}

func TestGetReferenceBlobsMissingAnnotation(t *testing.T) {
	record := &Record{
		NydusBootstrapDesc: &ocispec.Descriptor{
			Annotations: map[string]string{},
		},
	}
	result := record.GetReferenceBlobs()
	assert.Equal(t, []string{}, result)
}

func TestGetReferenceBlobsNilAnnotations(t *testing.T) {
	record := &Record{
		NydusBootstrapDesc: &ocispec.Descriptor{
			Annotations: nil,
		},
	}
	result := record.GetReferenceBlobs()
	assert.Equal(t, []string{}, result)
}

func TestGetReferenceBlobsMalformedJSON(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
	}{
		{"invalid json", `not json`},
		{"object instead of array", `{"key":"val"}`},
		{"truncated", `["blob1`},
		{"number array", `[1,2,3]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &Record{
				NydusBootstrapDesc: &ocispec.Descriptor{
					Annotations: map[string]string{
						utils.LayerAnnotationNydusReferenceBlobIDs: tt.jsonStr,
					},
				},
			}
			result := record.GetReferenceBlobs()
			assert.Equal(t, []string{}, result)
		})
	}
}

func TestNewCache(t *testing.T) {
	cache, err := New(nil, Opt{
		MaxRecords:     10,
		Version:        "v1",
		FsVersion:      "6",
		DockerV2Format: false,
	})
	assert.Nil(t, err)
	assert.NotNil(t, cache)
	assert.NotNil(t, cache.pulledRecords)
	assert.NotNil(t, cache.referenceRecords)
	assert.NotNil(t, cache.pushedRecords)
	assert.Equal(t, uint(10), cache.opt.MaxRecords)
	assert.Equal(t, "v1", cache.opt.Version)
	assert.Equal(t, "6", cache.opt.FsVersion)
}

func TestCacheRecordLimitsToMaxRecords(t *testing.T) {
	cache, err := New(nil, Opt{
		MaxRecords: 2,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})
	assert.Nil(t, err)

	records := []*Record{
		makeRecord(1, true),
		makeRecord(2, true),
		makeRecord(3, true),
	}
	cache.Record(records)
	assert.Equal(t, 2, len(cache.pushedRecords))
}

func TestCacheRecordMoveFront(t *testing.T) {
	cache, err := New(nil, Opt{
		MaxRecords: 5,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})
	assert.Nil(t, err)

	cache.Record([]*Record{makeRecord(1, true), makeRecord(2, true)})
	assert.Equal(t, 2, len(cache.pushedRecords))

	// Record same item again moves it to front, old duplicate is skipped
	cache.Record([]*Record{makeRecord(1, true)})
	// Result: [record(1)] + [record(2)] from old (record(1) skipped as duplicate) = 2
	assert.Equal(t, 2, len(cache.pushedRecords))
}

func TestCacheGetReferenceNotFound(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})
	result := cache.GetReference(digest.FromString("nonexistent"))
	assert.Nil(t, result)
}

func TestCacheSetAndGetReference(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := &ocispec.Descriptor{
		MediaType: utils.MediaTypeNydusBlob,
		Digest:    digest.FromString("test-blob"),
		Size:      100,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusBlob: "true",
		},
	}
	cache.SetReference(layer)

	result := cache.GetReference(layer.Digest)
	assert.NotNil(t, result)
	assert.NotNil(t, result.NydusBlobDesc)
	assert.Equal(t, layer.Digest, result.NydusBlobDesc.Digest)
}

func TestMergeRecordNilOld(t *testing.T) {
	newRec := makeRecord(1, true)
	result := mergeRecord(nil, newRec)
	assert.Equal(t, newRec.SourceChainID, result.SourceChainID)
	assert.Equal(t, newRec.NydusBootstrapDesc, result.NydusBootstrapDesc)
	assert.Equal(t, newRec.NydusBlobDesc, result.NydusBlobDesc)
}

func TestMergeRecordUpdatesBootstrap(t *testing.T) {
	oldRec := makeRecord(1, true)
	newRec := makeRecord(2, false)
	newRec.SourceChainID = oldRec.SourceChainID

	result := mergeRecord(oldRec, newRec)
	assert.Equal(t, newRec.NydusBootstrapDesc, result.NydusBootstrapDesc)
	// blob should remain from old since new has no blob
	assert.Equal(t, oldRec.NydusBlobDesc, result.NydusBlobDesc)
}

func TestMergeRecordUpdatesBlob(t *testing.T) {
	oldRec := makeRecord(1, false)
	newRec := &Record{
		SourceChainID: oldRec.SourceChainID,
		NydusBlobDesc: &ocispec.Descriptor{
			MediaType: utils.MediaTypeNydusBlob,
			Digest:    digest.FromString("new-blob"),
			Size:      999,
		},
	}

	result := mergeRecord(oldRec, newRec)
	assert.Equal(t, newRec.NydusBlobDesc, result.NydusBlobDesc)
	// bootstrap should remain from old
	assert.Equal(t, oldRec.NydusBootstrapDesc, result.NydusBootstrapDesc)
}

func TestLayerToRecordBlobLayer(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	blobLayer := makeBlobLayer(1)
	record := cache.layerToRecord(&blobLayer)
	assert.NotNil(t, record)
	assert.Equal(t, digest.FromString("chain-1"), record.SourceChainID)
	assert.NotNil(t, record.NydusBlobDesc)
	assert.Nil(t, record.NydusBootstrapDesc)
}

func TestLayerToRecordBootstrapLayer(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	bootstrapLayer := makeBootstrapLayer(1, false)
	record := cache.layerToRecord(&bootstrapLayer)
	assert.NotNil(t, record)
	assert.Equal(t, digest.FromString("chain-1"), record.SourceChainID)
	assert.NotNil(t, record.NydusBootstrapDesc)
	assert.Nil(t, record.NydusBlobDesc)
}

func TestLayerToRecordBootstrapWithBlob(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	bootstrapLayer := makeBootstrapLayer(1, true)
	record := cache.layerToRecord(&bootstrapLayer)
	assert.NotNil(t, record)
	assert.NotNil(t, record.NydusBootstrapDesc)
	assert.NotNil(t, record.NydusBlobDesc)
}

func TestLayerToRecordInvalidSourceChainID(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: "not-a-valid-digest",
			utils.LayerAnnotationNydusBootstrap:     "true",
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestLayerToRecordNoAnnotations(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType:   ocispec.MediaTypeImageLayerGzip,
		Annotations: nil,
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestLayerToRecordReferenceBlobNoSourceChainID(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: utils.MediaTypeNydusBlob,
		Digest:    digest.FromString("ref-blob"),
		Size:      100,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusBlob: "true",
		},
	}
	record := cache.layerToRecord(&layer)
	assert.NotNil(t, record)
	assert.Equal(t, digest.Digest(""), record.SourceChainID)
	assert.NotNil(t, record.NydusBlobDesc)
}

func TestLayerToRecordMissingUncompressedDigest(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain").String(),
			utils.LayerAnnotationNydusBootstrap:     "true",
			// Missing LayerAnnotationUncompressed
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestImportRecordsFromLayers(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layers := []ocispec.Descriptor{
		makeBootstrapLayer(1, false),
		makeBlobLayer(1),
		makeBootstrapLayer(2, false),
	}
	cache.importRecordsFromLayers(layers)

	// pulledRecords is a map keyed by SourceChainID, so 2 unique chain IDs
	assert.Equal(t, 2, len(cache.pulledRecords))
	// pushedRecords gets an append for each layer processed, so 3 entries
	assert.Equal(t, 3, len(cache.pushedRecords))
}

func TestManifestStruct(t *testing.T) {
	m := Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
	}
	assert.Equal(t, ocispec.MediaTypeImageManifest, m.MediaType)
}

func TestRecordStruct(t *testing.T) {
	rec := Record{
		SourceChainID:        digest.FromString("chain"),
		NydusBootstrapDiffID: digest.FromString("diff"),
	}
	assert.NotEmpty(t, rec.SourceChainID)
	assert.NotEmpty(t, rec.NydusBootstrapDiffID)
	assert.Nil(t, rec.NydusBlobDesc)
	assert.Nil(t, rec.NydusBootstrapDesc)
}

func TestRecordToLayerRegistryBackendWithReferenceBlobs(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	record := makeRecord(1, true)
	record.NydusBootstrapDesc.Annotations = map[string]string{
		utils.LayerAnnotationNydusReferenceBlobIDs: `["ref1","ref2"]`,
	}
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.NotNil(t, bootstrapDesc)
	assert.NotNil(t, blobDesc)
	assert.Contains(t, bootstrapDesc.Annotations[utils.LayerAnnotationNydusReferenceBlobIDs], "ref1")
}

func TestRecordToLayerReferenceBlobRegistryBackend(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	// Reference record (no SourceChainID)
	record := &Record{
		NydusBlobDesc: &ocispec.Descriptor{
			MediaType: utils.MediaTypeNydusBlob,
			Digest:    digest.FromString("ref-blob"),
			Size:      100,
		},
	}
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.Nil(t, bootstrapDesc)
	assert.NotNil(t, blobDesc)
	assert.Equal(t, utils.MediaTypeNydusBlob, blobDesc.MediaType)
}

func TestRecordToLayerReferenceBlobOSSBackend(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	record := &Record{
		NydusBlobDesc: &ocispec.Descriptor{
			MediaType: utils.MediaTypeNydusBlob,
			Digest:    digest.FromString("ref-blob"),
			Size:      100,
		},
	}
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.Nil(t, bootstrapDesc)
	assert.Nil(t, blobDesc)
}

func TestRecordToLayerReferenceBlobNilBlob(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	record := &Record{}
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.Nil(t, bootstrapDesc)
	assert.Nil(t, blobDesc)
}

func TestRecordToLayerDockerV2Format(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords:     10,
		Backend:        &backend.Registry{},
		FsVersion:      "6",
		DockerV2Format: true,
	})

	record := makeRecord(1, true)
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.NotNil(t, bootstrapDesc)
	assert.NotNil(t, blobDesc)
	// Docker v2 format should use docker schema media type
	assert.Equal(t, "application/vnd.docker.image.rootfs.diff.tar.gzip", bootstrapDesc.MediaType)
}

func TestLayerToRecordInvalidBlobDigest(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromString("bootstrap"),
		Size:      100,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain").String(),
			utils.LayerAnnotationNydusBootstrap:     "true",
			utils.LayerAnnotationUncompressed:       digest.FromString("uncompressed").String(),
			utils.LayerAnnotationNydusBlobDigest:    "invalid-digest",
			utils.LayerAnnotationNydusBlobSize:      "100",
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestLayerToRecordInvalidBlobSize(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Digest:    digest.FromString("bootstrap"),
		Size:      100,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain").String(),
			utils.LayerAnnotationNydusBootstrap:     "true",
			utils.LayerAnnotationUncompressed:       digest.FromString("uncompressed").String(),
			utils.LayerAnnotationNydusBlobDigest:    digest.FromString("blob").String(),
			utils.LayerAnnotationNydusBlobSize:      "not-a-number",
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestLayerToRecordInvalidUncompressedDigest(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain").String(),
			utils.LayerAnnotationNydusBootstrap:     "true",
			utils.LayerAnnotationUncompressed:       "invalid-digest",
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestLayerToRecordNonBlobNonBootstrap(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	layer := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusSourceChainID: digest.FromString("chain").String(),
		},
	}
	record := cache.layerToRecord(&layer)
	assert.Nil(t, record)
}

func TestExportRecordsToLayersWithReferenceBlobs(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	// Set up a reference blob
	refBlobDigest := digest.FromString("ref-blob")
	cache.referenceRecords[refBlobDigest] = &Record{
		NydusBlobDesc: &ocispec.Descriptor{
			MediaType: utils.MediaTypeNydusBlob,
			Digest:    refBlobDigest,
			Size:      500,
		},
	}

	// Create a record that references the blob
	record := makeRecord(1, true)
	record.NydusBootstrapDesc.Annotations = map[string]string{
		utils.LayerAnnotationNydusReferenceBlobIDs: `["` + refBlobDigest.Encoded() + `"]`,
	}
	cache.pushedRecords = []*Record{record}

	layers := cache.exportRecordsToLayers()
	assert.NotEmpty(t, layers)
	// Should have reference blob layer first, then bootstrap + blob layers
	assert.True(t, len(layers) >= 2)
}

func TestImportRecordsWithReferenceBlobs(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.Registry{},
		FsVersion:  "6",
	})

	// Create a reference blob layer (no SourceChainID)
	refLayer := ocispec.Descriptor{
		MediaType: utils.MediaTypeNydusBlob,
		Digest:    digest.FromString("ref-blob"),
		Size:      100,
		Annotations: map[string]string{
			utils.LayerAnnotationNydusBlob: "true",
		},
	}

	layers := []ocispec.Descriptor{
		refLayer,
		makeBootstrapLayer(1, false),
	}
	cache.importRecordsFromLayers(layers)

	assert.Equal(t, 1, len(cache.referenceRecords))
	assert.Equal(t, 1, len(cache.pulledRecords))
}

func TestCacheRecordExactlyMaxRecords(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 3,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	records := []*Record{
		makeRecord(1, true),
		makeRecord(2, true),
		makeRecord(3, true),
	}
	cache.Record(records)
	assert.Equal(t, 3, len(cache.pushedRecords))
}

func TestRecordToLayerOSSBackendBlobAnnotations(t *testing.T) {
	cache, _ := New(nil, Opt{
		MaxRecords: 10,
		Backend:    &backend.OSSBackend{},
		FsVersion:  "6",
	})

	record := makeRecord(1, true)
	bootstrapDesc, blobDesc := cache.recordToLayer(record)
	assert.NotNil(t, bootstrapDesc)
	assert.Nil(t, blobDesc) // OSS backend doesn't produce blob desc
	// Instead, blob info is in bootstrap annotations
	assert.NotEmpty(t, bootstrapDesc.Annotations[utils.LayerAnnotationNydusBlobDigest])
	assert.NotEmpty(t, bootstrapDesc.Annotations[utils.LayerAnnotationNydusBlobSize])
}
