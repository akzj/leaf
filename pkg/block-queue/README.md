# block-queue
goroutine safe block-queue



```go
goos: linux
goarch: amd64
pkg: github.com/akzj/block-queue
BenchmarkPushPopAll
items/second 0
items/second 0
items/second 19723861
items/second 21259660
items/second 21899868
BenchmarkPushPopAll-4   	25501527	        45.7 ns/op	       8 B/op	       0 allocs/op
PASS
```

```bash
goos: linux
goarch: amd64
pkg: github.com/akzj/block-queue
BenchmarkPushPop
items/second 11335295
BenchmarkPushPop-4   	50000000	        80.1 ns/op	      38 B/op	       1 allocs/op
PASS
```