module github.com/textileio/go-textile-bots

go 1.12

replace github.com/textileio/go-textile-core => ../go-textile-core

require (
	github.com/hashicorp/go-hclog v0.9.2
	github.com/hashicorp/go-plugin v1.0.1
	github.com/ipfs/go-datastore v0.1.1
	github.com/textileio/go-textile-core v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.0.0-20191014212845-da9a3fd4c582
	google.golang.org/grpc v1.24.0
)
