Simple alphabetical file index

```
fileIndex, e := cbindex.NewFileIndex(cbindex.Config{
    DataCsvPath:           "rdd/data.csv", // path to csv file with key to search as first column
    IndexCsvPath:          "rdd/index.csv", // path to csv file with partial key (same as IndexKeyLength) and byte offset of the start in the file
    ConcurrentHandleLimit: 10000, // maximum open file hands, will return errors if exceeded
    WarmUpCount:           5000, // how many file handles to warm up if fileIndex.WarmUp() is called
    WarmUpDelay:           time.Millisecond, // how long to wait between warming each handle
    IndexKeyLength:        3, // character length of the indexes
})
```