BINARY_NAME=novab2tile
now=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
all: mac linux windows

mac: mac_amd64 mac_arm64

linux: linux_amd64 linux_arm64

windows: windows_amd64 windows_arm64

mac_amd64:
	GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-darwin-amd64 main.go

mac_arm64:
	GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-darwin-arm64 main.go

linux_amd64:
	GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-linux-amd64 main.go

linux_arm64:
	GOOS=linux GOARCH=arm64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-linux-arm64 main.go

windows_amd64:
	GOOS=windows GOARCH=amd64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-windows-amd64.exe main.go

windows_arm64:
	GOOS=windows GOARCH=arm64 go build -ldflags "-X main.VERSION=${now}" -o bin/$(BINARY_NAME)-windows-arm64.exe main.go

clean:
	rm -f bin/*