build:
	go build -o kv main.go

run:
	./kv ./data -raddr localhost:10001 -haddr localhost:10002 -inmem false -id node0 

clean:
	rm -f kv

test:
	go test ./...