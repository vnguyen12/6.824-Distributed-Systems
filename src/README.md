How to Run Test Scripts


## Lab 1: MapReduce
```
  cd ~6.824/src/main    
  go build -race -buildmode=plugin ../mrapps/wc.go
  rm mr-out*
  go run -race mrsequential.go wc.so pg*.txt
  
  go run -race mrcoordinator.go pg-*.tx   // Run parallel coordinator and workers
  go run -race mrworker.go wc.so
  cat mr-out-* | sort | more              // Look at outputs
  
  bash test-mr.sh         // Run whole test suite
```

## Lab 2: Raft
```
  cd ~6.824/src/raft
  go test -run 2A -race   // Run only 2A tests
  go test -race           // Run whole test suite
  time go test -run 2B    // Measure CPU time requires to run tests
```
