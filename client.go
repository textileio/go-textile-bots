package textilebots

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	shared "github.com/textileio/go-textile-core/bots"
)

type Client struct {
	BotID      string
	Name       string
	Service    shared.Service
	SharedConf shared.ClientConfig
	config     *plugin.ClientConfig
	client     *plugin.Client
}

// setup will configure the rpc server information for the bot
func (b *Client) Prepare(botID string, version int, name string, pth string, config shared.ClientConfig) {
	pluginMap := map[string]plugin.Plugin{
		botID: &TextileBot{}, // <- the TextileBot interface will always be the same.
	}

	// handshake for any bot version will remain the same. it will error if the bot program version doesn't match the config
	handshake := plugin.HandshakeConfig{
		ProtocolVersion:  uint(version),
		MagicCookieKey:   botID, // TODO: this should use the current IPFS hash of the bot program
		MagicCookieValue: name,
	}

	// https://github.com/hashicorp/go-plugin/blob/master/client.go#L108
	// We're a host. Start by launching the plugin process.
	b.config = &plugin.ClientConfig{
		HandshakeConfig: handshake,
		Plugins:         pluginMap,
		Cmd:             exec.Command("sh", "-c", pth),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolNetRPC, plugin.ProtocolGRPC},
		Managed: true,
		Logger: hclog.New(&hclog.LoggerOptions{
			Output: hclog.DefaultOutput,
			Level:  hclog.Error,
			Name:   "plugin",
		}),
		// TODO add SecureConfig *SecureConfig for Hash-based bots
	}
	b.client = plugin.NewClient(b.config)
	b.BotID = botID
	b.Name = name
	b.SharedConf = config
	// in go-plugin examples we need defer Kill, but because Managed: true, do we?
	// defer b.client.Kill()
	b.Run()
}

func (b *Client) Run() {
	// defer magicLink.client.Kill()
	// Connect via RPC
	rpcClient, err := b.client.Client()
	if err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
	// Request the plugin
	raw, err := rpcClient.Dispense(b.BotID)
	if err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
	// We should have a Bot service store now! This feels like a normal interface
	// implementation but is in fact over an RPC connection.
	b.Service = raw.(shared.Service)
}
