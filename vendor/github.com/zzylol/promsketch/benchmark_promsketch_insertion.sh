go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=1
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10 
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=100
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=1000 
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=10000
go test -v  -count=5  -timeout 0 -run TestInsertThroughput ./ -numts=100000