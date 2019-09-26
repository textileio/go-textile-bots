module github.com/textileio/go-textile-bots

go 1.12

replace github.com/textileio/go-textile-core v0.0.1 => ../go-textile-core

require (
	github.com/hashicorp/go-hclog v0.9.2
	github.com/hashicorp/go-plugin v1.0.1
	github.com/textileio/go-textile-core v0.0.1
	golang.org/x/net v0.0.0-20190926025831-c00fd9afed17
	google.golang.org/grpc v1.24.0
)
