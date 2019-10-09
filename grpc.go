package textilebots

import (
	hclog "github.com/hashicorp/go-hclog"
	plugin "github.com/hashicorp/go-plugin"
	shared "github.com/textileio/go-textile-core/bots"
	proto "github.com/textileio/go-textile-core/bots/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct {
	broker *plugin.GRPCBroker
	client proto.BotserviceClient
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCBotStoreServer struct {
	// This is the real implementation
	Impl shared.Store
}
type GRPCIpfsHandlerServer struct {
	// This is the real implementation
	Impl shared.Ipfs
}

func (m *GRPCClient) Delete(q []byte, config shared.ClientConfig) (shared.Response, error) {
	botStoreServer := &GRPCBotStoreServer{Impl: config.Store}
	var s *grpc.Server
	storeServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s = grpc.NewServer(opts...)
		proto.RegisterBotStoreServer(s, botStoreServer)

		return s
	}
	storeBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(storeBrokerID, storeServerFunc)

	ipfsHandlerServer := &GRPCIpfsHandlerServer{Impl: config.Ipfs}
	var s2 *grpc.Server
	ipfsServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s2 = grpc.NewServer(opts...)
		proto.RegisterIpfsHandlerServer(s2, ipfsHandlerServer)

		return s2
	}
	ipfsBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(ipfsBrokerID, ipfsServerFunc)

	resp, err := m.client.Delete(context.Background(), &proto.APIRequest{
		Data: q,
		Setup: &proto.ClientConfig{
			BotStoreServer:    storeBrokerID,
			IpfsHandlerServer: ipfsBrokerID,
			Params:            config.Params,
		},
	})

	s.Stop()
	s2.Stop()

	if err != nil {
		return shared.Response{}, err
	}

	return shared.Response{
		Status:      resp.Status,
		Body:        resp.Body,
		ContentType: resp.ContentType,
	}, nil
}

func (m *GRPCClient) Put(q []byte, b []byte, config shared.ClientConfig) (shared.Response, error) {

	botStoreServer := &GRPCBotStoreServer{Impl: config.Store}
	var s *grpc.Server
	storeServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s = grpc.NewServer(opts...)
		proto.RegisterBotStoreServer(s, botStoreServer)

		return s
	}
	storeBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(storeBrokerID, storeServerFunc)

	ipfsHandlerServer := &GRPCIpfsHandlerServer{Impl: config.Ipfs}
	var s2 *grpc.Server
	ipfsServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s2 = grpc.NewServer(opts...)
		proto.RegisterIpfsHandlerServer(s2, ipfsHandlerServer)

		return s2
	}
	ipfsBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(ipfsBrokerID, ipfsServerFunc)

	resp, err := m.client.Put(context.Background(), &proto.APIRequestB{
		Data: q,
		Body: b,
		Setup: &proto.ClientConfig{
			BotStoreServer:    storeBrokerID,
			IpfsHandlerServer: ipfsBrokerID,
			Params:            config.Params,
		},
	})

	s.Stop()
	s2.Stop()

	if err != nil {
		return shared.Response{}, err
	}

	return shared.Response{
		Status:      resp.Status,
		Body:        resp.Body,
		ContentType: resp.ContentType,
	}, nil
}

func (m *GRPCClient) Post(q []byte, b []byte, config shared.ClientConfig) (shared.Response, error) {

	botStoreServer := &GRPCBotStoreServer{Impl: config.Store}
	var s *grpc.Server
	storeServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s = grpc.NewServer(opts...)
		proto.RegisterBotStoreServer(s, botStoreServer)

		return s
	}
	storeBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(storeBrokerID, storeServerFunc)

	ipfsHandlerServer := &GRPCIpfsHandlerServer{Impl: config.Ipfs}
	var s2 *grpc.Server
	ipfsServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s2 = grpc.NewServer(opts...)
		proto.RegisterIpfsHandlerServer(s2, ipfsHandlerServer)

		return s2
	}
	ipfsBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(ipfsBrokerID, ipfsServerFunc)

	resp, err := m.client.Post(context.Background(), &proto.APIRequestB{
		Data: q,
		Body: b,
		Setup: &proto.ClientConfig{
			BotStoreServer:    storeBrokerID,
			IpfsHandlerServer: ipfsBrokerID,
			Params:            config.Params,
		},
	})

	s.Stop()
	s2.Stop()

	if err != nil {
		return shared.Response{}, err
	}

	return shared.Response{
		Status:      resp.Status,
		Body:        resp.Body,
		ContentType: resp.ContentType,
	}, nil
}

func (m *GRPCClient) Get(q []byte, config shared.ClientConfig) (shared.Response, error) {

	botStoreServer := &GRPCBotStoreServer{Impl: config.Store}
	var s *grpc.Server
	storeServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s = grpc.NewServer(opts...)
		proto.RegisterBotStoreServer(s, botStoreServer)

		return s
	}
	storeBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(storeBrokerID, storeServerFunc)

	ipfsHandlerServer := &GRPCIpfsHandlerServer{Impl: config.Ipfs}
	var s2 *grpc.Server
	ipfsServerFunc := func(opts []grpc.ServerOption) *grpc.Server {
		s2 = grpc.NewServer(opts...)
		proto.RegisterIpfsHandlerServer(s2, ipfsHandlerServer)

		return s2
	}
	ipfsBrokerID := m.broker.NextId()
	go m.broker.AcceptAndServe(ipfsBrokerID, ipfsServerFunc)

	resp, err := m.client.Get(context.Background(), &proto.APIRequest{
		Data: q,
		Setup: &proto.ClientConfig{
			BotStoreServer:    storeBrokerID,
			IpfsHandlerServer: ipfsBrokerID,
			Params:            config.Params,
		},
	})

	s.Stop()
	s2.Stop()

	if err != nil {
		return shared.Response{}, err
	}

	return shared.Response{
		Status:      resp.Status,
		Body:        resp.Body,
		ContentType: resp.ContentType,
	}, nil
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl shared.Service

	broker *plugin.GRPCBroker
}

type GRPCIpfsHandlerClient struct{ client proto.IpfsHandlerClient }
type GRPCBotStoreClient struct{ client proto.BotStoreClient }

func (m *GRPCServer) Delete(ctx context.Context, req *proto.APIRequest) (*proto.BotResponse, error) {

	conn, err := m.broker.Dial(req.Setup.BotStoreServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	s := &GRPCBotStoreClient{proto.NewBotStoreClient(conn)}

	conn2, err := m.broker.Dial(req.Setup.IpfsHandlerServer)
	if err != nil {
		return nil, err
	}
	defer conn2.Close()
	i := &GRPCIpfsHandlerClient{proto.NewIpfsHandlerClient(conn2)}
	setup := shared.ClientConfig{
		s,
		i,
		req.Setup.Params,
	}
	res, err := m.Impl.Delete(req.Data, setup)
	if err != nil {
		return nil, err
	}
	return &proto.BotResponse{Status: res.Status, Body: res.Body, ContentType: res.ContentType}, nil
}

func (m *GRPCServer) Put(ctx context.Context, req *proto.APIRequestB) (*proto.BotResponse, error) {

	conn, err := m.broker.Dial(req.Setup.BotStoreServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	s := &GRPCBotStoreClient{proto.NewBotStoreClient(conn)}

	conn2, err := m.broker.Dial(req.Setup.IpfsHandlerServer)
	if err != nil {
		return nil, err
	}
	defer conn2.Close()
	i := &GRPCIpfsHandlerClient{proto.NewIpfsHandlerClient(conn2)}

	setup := shared.ClientConfig{
		s,
		i,
		req.Setup.Params,
	}
	res, err := m.Impl.Put(req.Data, req.Body, setup)
	if err != nil {
		return nil, err
	}
	return &proto.BotResponse{Status: res.Status, Body: res.Body, ContentType: res.ContentType}, nil
}

func (m *GRPCServer) Post(ctx context.Context, req *proto.APIRequestB) (*proto.BotResponse, error) {

	conn, err := m.broker.Dial(req.Setup.BotStoreServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	s := &GRPCBotStoreClient{proto.NewBotStoreClient(conn)}

	conn2, err := m.broker.Dial(req.Setup.IpfsHandlerServer)
	if err != nil {
		return nil, err
	}
	defer conn2.Close()
	i := &GRPCIpfsHandlerClient{proto.NewIpfsHandlerClient(conn2)}

	setup := shared.ClientConfig{
		s,
		i,
		req.Setup.Params,
	}
	res, err := m.Impl.Post(req.Data, req.Body, setup)
	if err != nil {
		return nil, err
	}
	return &proto.BotResponse{Status: res.Status, Body: res.Body, ContentType: res.ContentType}, nil
}

func (m *GRPCServer) Get(ctx context.Context, req *proto.APIRequest) (*proto.BotResponse, error) {

	conn, err := m.broker.Dial(req.Setup.BotStoreServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	s := &GRPCBotStoreClient{proto.NewBotStoreClient(conn)}

	conn2, err := m.broker.Dial(req.Setup.IpfsHandlerServer)
	if err != nil {
		return nil, err
	}
	defer conn2.Close()
	i := &GRPCIpfsHandlerClient{proto.NewIpfsHandlerClient(conn2)}

	setup := shared.ClientConfig{
		s,
		i,
		req.Setup.Params,
	}
	res, err := m.Impl.Get(req.Data, setup)
	if err != nil {
		return nil, err
	}
	return &proto.BotResponse{Status: res.Status, Body: res.Body, ContentType: res.ContentType}, nil
}

// IpfsHandler Client
func (m *GRPCIpfsHandlerClient) Get(path string, key string) ([]byte, error) {
	resp, err := m.client.Get(context.Background(), &proto.GetData{
		Path: path,
		Key:  key,
	})
	if err != nil {
		hclog.Default().Info("ipfs.Get", "client", "start", "err", err)
		return nil, err
	}
	return resp.Data, err
}

func (m *GRPCIpfsHandlerClient) Add(data []byte, encrypt bool) (string, string, error) {
	resp, err := m.client.Add(context.Background(), &proto.AddData{
		Data:    data,
		Encrypt: encrypt,
	})
	if err != nil {
		hclog.Default().Info("ipfs.Get", "client", "start", "err", err)
		return "", "", err
	}
	return resp.Hash, resp.Key, err
}

// IpfsHandler Server
func (m *GRPCIpfsHandlerServer) Get(ctx context.Context, req *proto.GetData) (resp *proto.ByteData, err error) {
	d, err := m.Impl.Get(req.Path, req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.ByteData{Data: d}, err
}

func (m *GRPCIpfsHandlerServer) Add(ctx context.Context, req *proto.AddData) (resp *proto.IPFSPin, err error) {
	h, k, err := m.Impl.Add(req.Data, req.Encrypt)
	if err != nil {
		return nil, err
	}
	return &proto.IPFSPin{Hash: h, Key: k}, err
}

// BotStore Client
func (m *GRPCBotStoreClient) Get(key string) ([]byte, int32, error) {
	resp, err := m.client.Get(context.Background(), &proto.ByKey{
		Key: key,
	})
	if err != nil {
		hclog.Default().Info("store.Get", "client", "start", "err", err)
		return nil, 0, err
	}
	return resp.Data, resp.Version, err
}

func (m *GRPCBotStoreClient) Delete(key string) (bool, error) {
	resp, err := m.client.Delete(context.Background(), &proto.ByKey{
		Key: key,
	})
	if err != nil {
		hclog.Default().Info("store.Delete", "client", "start", "err", err)
		return false, err
	}
	return resp.Success, err
}

func (m *GRPCBotStoreClient) Set(key string, data []byte) (bool, error) {
	resp, err := m.client.Set(context.Background(), &proto.SetByKey{
		Key:  key,
		Data: data,
	})
	if err != nil {
		hclog.Default().Info("store.Set", "client", "start", "err", err)
		return false, err
	}
	return resp.Success, err
}

// BotStore Server
func (m *GRPCBotStoreServer) Get(ctx context.Context, req *proto.ByKey) (resp *proto.KeyValResponse, err error) {
	d, v, err := m.Impl.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.KeyValResponse{Data: d, Version: v}, err
}

func (m *GRPCBotStoreServer) Delete(ctx context.Context, req *proto.ByKey) (resp *proto.Success, err error) {
	s, err := m.Impl.Delete(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Success{Success: s}, err
}

func (m *GRPCBotStoreServer) Set(ctx context.Context, req *proto.SetByKey) (resp *proto.Success, err error) {
	s, err := m.Impl.Set(req.Key, req.Data)
	if err != nil {
		return nil, err
	}
	return &proto.Success{Success: s}, err
}
