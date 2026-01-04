.PHONY: build clean

# Build the gateway binary to the bin folder
build:
	@mkdir -p bin
	go build -o bin/gateway ./cmd/gateway

# Clean the bin folder
clean:
	rm -rf bin

