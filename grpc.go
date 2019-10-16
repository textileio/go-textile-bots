package textilebots

import (
	hclog "github.com/hashicorp/go-hclog"
	plugin "github.com/hashicorp/go-plugin"
	"github.com/ipfs/go-datastore"
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
	Impl shared.DatastoreWithoutQuery
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
func (m *GRPCBotStoreClient) Get(key datastore.Key) ([]byte, error) {
	resp, err := m.client.Get(context.Background(), &proto.DatastoreKey{
		Struct: &proto.StructKey{Key: key.String()},
	})
	if err != nil {
		hclog.Default().Info("store.Get", "client", "start", "err", err)
		return []byte{}, err
	}
	return resp.Data, err
}

func (m *GRPCBotStoreClient) GetSize(key datastore.Key) (int, error) {
	resp, err := m.client.GetSize(context.Background(), &proto.DatastoreKey{
		Struct: &proto.StructKey{Key: key.String()},
	})
	if err != nil {
		hclog.Default().Info("store.GetSize", "client", "start", "err", err)
		return 0, err
	}
	return int(resp.Size), err
}

func (m *GRPCBotStoreClient) Has(key datastore.Key) (bool, error) {
	resp, err := m.client.Has(context.Background(), &proto.DatastoreKey{
		Struct: &proto.StructKey{Key: key.String()},
	})
	if err != nil {
		hclog.Default().Info("store.Has", "client", "start", "err", err)
		return false, err
	}
	return resp.Exists, err
}

func (m *GRPCBotStoreClient) Delete(key datastore.Key) error {
	_, err := m.client.Delete(context.Background(), &proto.DatastoreKey{
		Struct: &proto.StructKey{Key: key.String()},
	})
	if err != nil {
		hclog.Default().Info("store.Delete", "client", "start", "err", err)
	}
	return err
}

func (m *GRPCBotStoreClient) Put(key datastore.Key, value []byte) error {
	_, err := m.client.Put(context.Background(), &proto.DatastoreKeyValue{
		Key:   key.String(),
		Value: value,
	})
	if err != nil {
		hclog.Default().Info("store.Set", "client", "start", "err", err)
	}
	return err
}

func (m *GRPCBotStoreClient) Close() error {
	_, err := m.client.Close(context.Background(), &proto.Empty{})
	if err != nil {
		hclog.Default().Info("store.Set", "client", "start", "err", err)
	}
	return err
}

// BotStore Server
func (m *GRPCBotStoreServer) Get(ctx context.Context, req *proto.DatastoreKey) (resp *proto.KeyValResponse, err error) {
	key := datastore.NewKey(req.Struct.Key)
	d, err := m.Impl.Get(key)
	if err != nil {
		return &proto.KeyValResponse{}, err
	}
	return &proto.KeyValResponse{Data: d}, err
}

func (m *GRPCBotStoreServer) GetSize(ctx context.Context, req *proto.DatastoreKey) (resp *proto.DatastoreSize, err error) {
	key := datastore.NewKey(req.Struct.Key)
	s, err := m.Impl.GetSize(key)
	if err != nil {
		return &proto.DatastoreSize{}, err
	}
	return &proto.DatastoreSize{Size: int32(s)}, err
}

func (m *GRPCBotStoreServer) Has(ctx context.Context, req *proto.DatastoreKey) (resp *proto.Exists, err error) {
	key := datastore.NewKey(req.Struct.Key)
	ex, err := m.Impl.Has(key)
	if err != nil {
		return &proto.Exists{}, err
	}
	return &proto.Exists{Exists: ex}, err
}

func (m *GRPCBotStoreServer) Delete(ctx context.Context, req *proto.DatastoreKey) (resp *proto.Empty, err error) {
	key := datastore.NewKey(req.Struct.Key)
	return &proto.Empty{}, m.Impl.Delete(key)
}

func (m *GRPCBotStoreServer) Put(ctx context.Context, req *proto.DatastoreKeyValue) (resp *proto.Empty, err error) {
	key := datastore.NewKey(req.Key)
	return &proto.Empty{}, m.Impl.Put(key, req.Value)
}

func (m *GRPCBotStoreServer) Close(ctx context.Context, req *proto.Empty) (resp *proto.Empty, err error) {
	return &proto.Empty{}, m.Impl.Close()
}
