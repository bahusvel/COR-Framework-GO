package cor

import (
	"fmt"
	codec "github.com/ugorji/go/codec"
	"net"
	"strings"
)

type RouteCallback func(*Message) int32

func DstRouterFactory(default_route int32) RouteCallback {
	return func(msg *Message) int32{
		if len(msg.Destination) == 0 {
			return default_route
		} else {
			return msg.Destination[len(msg.Destination)-1]
		}
	}
}

func StaticRouterFactory(routes *map[string] int32) RouteCallback{
	return func(msg *Message) int32{
		if route, ok := (*routes)[msg.Atype]; ok {
			return route
		} else {
			return 0
		}
	}
}

type NetworkAdapter interface {
	Init(module *Module)
	MessageOut(msg *Message)
}

type SocketAdapter struct {
	module     	  *Module
	RouteCallback RouteCallback
	protocol	  string
	handle		  *codec.MsgpackHandle
	encoderMap 	  map[int32] *codec.Encoder
	connectionMap map[net.Conn] *codec.Encoder
}

func (this *SocketAdapter) MessageOut(msg *Message) {
	dst := this.RouteCallback(msg)
	if enc, ok := this.encoderMap[dst]; ok{
			enc.Encode(msg.ToMap())
	} else {
		fmt.Printf("Destination %d is unknown, will broadcast\n", dst)
		msg_map := msg.ToMap()
		fmt.Println(this.connectionMap)
		for _, enc := range this.connectionMap {
			enc.Encode(msg_map)
		}
	}

}

func (this *SocketAdapter) connectionHandler(conn net.Conn){
	dec := codec.NewDecoder(conn, this.handle)
	enc := codec.NewEncoder(conn, this.handle)
	this.connectionMap[conn] = enc
	for {
		msgmap := make(map[string]interface{})
		dec.Decode(&msgmap)
		msg := Message{}
		msg.FromMap(msgmap)
		if _, ok := this.encoderMap[msg.Source[0]]; !ok {
			delete(this.connectionMap, conn)
			this.encoderMap[msg.Source[0]] = enc
		}
		if handler, ok := this.module.consumes[msg.Atype]; ok {
			handler(&msg)
		}
	}
}

func (this *SocketAdapter) connectionListener(ln net.Listener){
	for {
		conn, err := ln.Accept()
		if err != nil {
		}
		go this.connectionHandler(conn)
	}

}

func (this *SocketAdapter) Init(module *Module) {
	fmt.Println("Initializing Socket Adapter")
	this.encoderMap = make(map[int32] *codec.Encoder)
	this.connectionMap = make(map[net.Conn] *codec.Encoder)
	this.module = module
	if this.RouteCallback == nil {
		this.RouteCallback = DstRouterFactory(0)
	}
	this.handle = &codec.MsgpackHandle{}
	this.handle.RawToString = true
	if this.protocol == "" {
		this.protocol = "tcp"
	}
	ln, err := net.Listen(this.protocol, ":6050")
	if err != nil {
		fmt.Println(err, ", will need to connect instead.")
	} else {
		go this.connectionListener(ln)
	}
}

func (this *SocketAdapter) Connect(moduleIf string) {
	parts := strings.Split(moduleIf, "://")
	var network string
	switch parts[0] {
	case "unixsock":
		network = "unix"
	case "tcp":
		network = "tcp"
	default:
		network = "tcp"
	}
	var err error
	fmt.Println("Connecting to ", parts[1])
	conn, err := net.Dial(network, parts[1])
	for err != nil {
		fmt.Errorf("Could not connect, %v", err)
		conn, err = net.Dial(network, parts[1])
	}
	go this.connectionHandler(conn)
	fmt.Println("Successfully connected to ", moduleIf)
}
