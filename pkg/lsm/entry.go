package lsm

import "github.com/engine/lsm-trees/pkg/core"

// Type aliases for core LSM data structures.
type (
	Entry          = core.Entry
	Iterator       = core.Iterator
	MergedIterator = core.MergedIterator
	Manifest       = core.Manifest
	TableMeta      = core.TableMeta
	VersionEdit    = core.VersionEdit
)

const (
	OpPut    = core.OpPut
	OpDelete = core.OpDelete
)

var (
	ErrKeyNotFound    = core.ErrKeyNotFound
	ErrKeyDeleted     = core.ErrKeyDeleted
	ErrEmptyKey       = core.ErrEmptyKey
	ErrDatabaseClosed = core.ErrDatabaseClosed
)

func NewPutEntry(key, value []byte) *Entry {
	return core.NewPutEntry(key, value)
}

func NewDeleteEntry(key []byte) *Entry {
	return core.NewDeleteEntry(key)
}

func DecodeEntry(data []byte) (*Entry, error) {
	return core.DecodeEntry(data)
}

func OpenManifest(dir string) (*Manifest, error) {
	return core.OpenManifest(dir)
}

func NewMergedIterator(iters []Iterator, dropTombstones bool) *MergedIterator {
	return core.NewMergedIterator(iters, dropTombstones)
}
