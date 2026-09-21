package core

// Iterator defines the uniform interface for iterating sorted key-value records.
type Iterator interface {
	Valid() bool
	Next()
	Seek(target []byte)
	Key() []byte
	Value() []byte
	Entry() *Entry
	Close() error
}
