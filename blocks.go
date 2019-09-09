package dumbfs // import "github.com/keks/dumbfs"

import (
	"io"
)

// Basic Types

// ReadWriterAt is both a ReaderAt and a WriterAt.
type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

// Block Layer

// BlockID identifies blocks.
type BlockID uint32

// Blocks manages the block layer of the dumb file system.
type Blocks interface {
	Get(BlockID) (ReadWriterAt, error)
	New() (BlockID, ReadWriterAt, error)
}

// Directory Layer

type FileName string

type Directory interface {
	Open(name FileName) File
}

// File Layer

type FileHint uint8

const (
	// FileHintStart indicates that this block is the beginning of a file
	FileHintStart = iota

	// FileHintCont indicates that this block is part of a file, but not the beginning
	FileHintCont

	// reserved

	// FileHintUserBase is the lowest 
	FileHintUserBase = 16
)

// File is a growable buffer, usually backed by blocks.
type File interface {
	Name() FileName
	Size() uint64
	// StartBlock() BlockID

	ReadWriterAt
}
