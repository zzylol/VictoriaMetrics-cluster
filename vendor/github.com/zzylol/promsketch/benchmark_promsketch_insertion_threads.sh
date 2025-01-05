go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=64
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=32
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=16
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=8
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=4
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=2
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000 -numthreads=1





