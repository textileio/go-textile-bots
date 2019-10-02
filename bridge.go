package textilebots

import (
	plugin "github.com/hashicorp/go-plugin"
	shared "github.com/textileio/go-textile-core/bots"
	proto "github.com/textileio/go-textile-core/bots/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// This is the implementation of plugin.Plugin so we can serve/consume this.
// We also implement GRPCPlugin so that this plugin can be served over
// gRPC.
type TextileBot struct {
	plugin.NetRPCUnsupportedPlugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl shared.Service
}

func (p *TextileBot) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterBotserviceServer(s, &GRPCServer{
		Impl:   p.Impl,
		broker: broker,
	})
	return nil
}

func (p *TextileBot) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: proto.NewBotserviceClient(c),
		broker: broker,
	}, nil
}

var _ plugin.GRPCPlugin = &TextileBot{}
