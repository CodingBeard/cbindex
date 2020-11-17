package cbindex

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	TooManyConcurrentHandlesError = errors.New("too many concurrent handles")
	InvalidHandleError            = errors.New("invalid handle")
	KeyTooShortForIndex           = errors.New("provided key too short for index")
)

type FileIndex struct {
	dataCsvPath           string
	indexCsvPath          string
	concurrentHandleLimit int32
	warmUp                bool
	warmUpCount           int
	warmUpDelay           time.Duration
	indexKeyLength        int

	pool            *sync.Pool
	openHandleCount int32
	index           map[string]int
}

type Config struct {
	DataCsvPath           string
	IndexCsvPath          string
	ConcurrentHandleLimit int32
	WarmUpCount           int
	WarmUpDelay           time.Duration
	IndexKeyLength        int
}

func NewFileIndex(config Config) (*FileIndex, error) {
	_, e := os.Stat(config.DataCsvPath)
	if errors.Is(e, os.ErrNotExist) {
		return nil, errors.New("data csv file does not exist")
	}
	_, e = os.Stat(config.IndexCsvPath)
	if errors.Is(e, os.ErrNotExist) {
		return nil, errors.New("index csv file does not exist")
	}

	indexFile, e := os.Open(config.IndexCsvPath)
	if e != nil {
		return nil, e
	}

	index := make(map[string]int)

	indexCsv := csv.NewReader(indexFile)
	for true {
		line, e := indexCsv.Read()
		if errors.Is(e, io.EOF) {
			break
		}

		key := line[0]
		offset, e := strconv.ParseInt(line[1], 10, 64)
		if e != nil {
			continue
		}

		index[key] = int(offset)
	}

	fileIndex := &FileIndex{
		dataCsvPath:           config.DataCsvPath,
		indexCsvPath:          config.IndexCsvPath,
		concurrentHandleLimit: config.ConcurrentHandleLimit,
		warmUpCount:           config.WarmUpCount,
		warmUpDelay:           config.WarmUpDelay,
		indexKeyLength:        config.IndexKeyLength,
		pool: &sync.Pool{
			New: func() interface{} {
				file, e := os.Open(config.DataCsvPath)
				if e != nil {
					return nil
				}
				return file
			},
		},
		index: index,
	}
	return fileIndex, nil
}

func (f *FileIndex) acquireHandle() (*os.File, error) {
	count := atomic.LoadInt32(&f.openHandleCount)
	if count > f.concurrentHandleLimit {
		return nil, TooManyConcurrentHandlesError
	}

	atomic.AddInt32(&f.openHandleCount, 1)

	handle := f.pool.Get().(*os.File)
	if handle == nil {
		return nil, InvalidHandleError
	}
	return handle, nil
}

func (f *FileIndex) releaseHandle(handle *os.File) {
	if handle != nil {
		atomic.AddInt32(&f.openHandleCount, -1)
		f.pool.Put(handle)
	}
}

func (f *FileIndex) WarmUp() error {
	var handles []*os.File
	for i := 0; i < f.warmUpCount; i++ {
		handle, e := f.acquireHandle()
		if e != nil {
			return e
		}
		handles = append(handles, handle)
		time.Sleep(f.warmUpDelay)
	}

	for _, handle := range handles {
		f.releaseHandle(handle)
	}

	return nil
}

func (f *FileIndex) GetRow(rowKey string) ([]string, error) {
	if len(rowKey) < f.indexKeyLength {
		return nil, KeyTooShortForIndex
	}
	indexKey := rowKey[:f.indexKeyLength]

	offset, ok := f.index[indexKey]

	if !ok {
		return nil, nil
	}

	handle, e := f.acquireHandle()
	if e != nil {
		return nil, e
	}
	defer f.releaseHandle(handle)

	_, e = handle.Seek(int64(offset), io.SeekStart)
	if e != nil {
		return nil, e
	}
	csvReader := csv.NewReader(handle)

	for true {
		line, e := csvReader.Read()

		if errors.Is(e, io.EOF) {
			break
		}

		if len(line[0]) < f.indexKeyLength {
			continue
		}
		key := line[0][:f.indexKeyLength]

		if key != indexKey {
			break
		}

		if line[0] == rowKey {
			return line, nil
		}
	}

	return nil, nil
}

func (f *FileIndex) GetRowsByPartialKey(rowKey string, limit int) ([][]string, error) {
	var rows [][]string
	if len(rowKey) < f.indexKeyLength {
		return rows, KeyTooShortForIndex
	}
	indexKey := rowKey[:f.indexKeyLength]

	offset, ok := f.index[indexKey]

	if !ok {
		return rows, nil
	}

	handle, e := f.acquireHandle()
	if e != nil {
		return rows, e
	}
	defer f.releaseHandle(handle)

	_, e = handle.Seek(int64(offset), io.SeekStart)
	if e != nil {
		return rows, e
	}

	csvReader := csv.NewReader(handle)

	for true {
		line, e := csvReader.Read()

		if errors.Is(e, io.EOF) {
			break
		}

		if len(line[0]) < f.indexKeyLength {
			continue
		}
		key := line[0][:f.indexKeyLength]

		if key != indexKey {
			break
		}

		if strings.Contains(strings.ToLower(line[0]), strings.ToLower(rowKey)) {
			rows = append(rows, line)
			if len(rows) >= limit && limit != -1 {
				return rows, nil
			}
		}
	}

	return rows, nil
}
