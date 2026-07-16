CARGO = cargo
DATA  = /tmp/raft-kv

.PHONY: build test clippy clean node1 node2 node3 integration-test

build:
	$(CARGO) build --workspace

test:
	$(CARGO) test --workspace

clippy:
	$(CARGO) clippy --workspace -- -D warnings

clean:
	rm -rf $(DATA)

node1:
	mkdir -p $(DATA)/node1
	RUST_LOG=info $(CARGO) run -p raft-kv -- \
		--id 1 \
		--grpc-addr 127.0.0.1:7001 \
		--http-addr 127.0.0.1:8001 \
		--peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003 \
		--http-peer 2=127.0.0.1:8002 --http-peer 3=127.0.0.1:8003 \
		--data-dir $(DATA)/node1

node2:
	mkdir -p $(DATA)/node2
	RUST_LOG=info $(CARGO) run -p raft-kv -- \
		--id 2 \
		--grpc-addr 127.0.0.1:7002 \
		--http-addr 127.0.0.1:8002 \
		--peer 1=127.0.0.1:7001 --peer 3=127.0.0.1:7003 \
		--http-peer 1=127.0.0.1:8001 --http-peer 3=127.0.0.1:8003 \
		--data-dir $(DATA)/node2

node3:
	mkdir -p $(DATA)/node3
	RUST_LOG=info $(CARGO) run -p raft-kv -- \
		--id 3 \
		--grpc-addr 127.0.0.1:7003 \
		--http-addr 127.0.0.1:8003 \
		--peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
		--http-peer 1=127.0.0.1:8001 --http-peer 2=127.0.0.1:8002 \
		--data-dir $(DATA)/node3

integration-test:
	bash scripts/integration_test.sh
