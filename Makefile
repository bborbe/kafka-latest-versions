
prepare:
	go get -u golang.org/x/tools/cmd/goimports
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u github.com/maxbrunsfeld/counterfeiter
	go get -u github.com/actgardner/gogen-avro/gogen-avro
	go get -u github.com/google/addlicense

ginkgo:
	go get github.com/onsi/ginkgo/ginkgo
	ginkgo -r -race -cover

precommit: ensure generate format addlicense test check
	@echo "ready to commit"

ensure:
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor

generate:
	go get github.com/maxbrunsfeld/counterfeiter
	go get github.com/actgardner/gogen-avro/gogen-avro
	rm -rf mocks avro
	go generate ./...

format:
	@go get golang.org/x/tools/cmd/goimports
	@find . -type f -name '*.go' -not -path './vendor/*' -exec gofmt -w "{}" +
	@find . -type f -name '*.go' -not -path './vendor/*' -exec goimports -w "{}" +

test:
	go test -cover -race $(shell go list ./... | grep -v /vendor/)

addlicense:
	go get github.com/google/addlicense
	addlicense -c "Benjamin Borbe" -y 2018 -l bsd ./*.go ./latestversion/*.go

check: lint vet errcheck

lint:
	@go get golang.org/x/lint/golint
	@golint -min_confidence 1 $(shell go list ./... | grep -v /vendor/)

vet:
	@go vet $(shell go list ./... | grep -v /vendor/)

errcheck:
	@go get github.com/kisielk/errcheck
	@errcheck -ignore '(Close|Write|Fprint)' $(shell go list ./... | grep -v /vendor/)
