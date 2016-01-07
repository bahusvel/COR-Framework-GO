package cor

import (
	"fmt"
	codec "github.com/ugorji/go/codec"
	"net"
	"strings"
)

type RouteCallback func(*Message) int32

func Dst_Router_Factory(default_route int32) RouteCallback {
	return func(msg *Message) int32{
		if len(msg.Destination) == 0 {
			return default_route
		} else {
			return msg.Destination[len(msg.Destination)-1]
		}
	}
}

type NetworkAdapter interface {
	Start()
	Init(module *Module, routeCallback RouteCallback)
	MessageOut(msg *Message)
}

type SocketAdapter struct {
	connected     bool
	conn          net.Conn
	enc           *codec.Encoder
	dec           *codec.Decoder
	module        *Module
	routeCallback RouteCallback
}

func (this *SocketAdapter) MessageOut(msg *Message) {
	this.enc.Encode(msg.ToMap())
}

func (this *SocketAdapter) ReceiveWorker() {
	for {
		msgmap := make(map[string]interface{})
		this.dec.Decode(&msgmap)
		msg := Message{}
		msg.FromMap(msgmap)
		if handler, ok := this.module.consumes[msg.Atype]; ok == true {
			handler(&msg)
		}
	}
}

func (this *SocketAdapter) Start() {

}

func (this *SocketAdapter) Init(module *Module, routeCallback RouteCallback) {
	fmt.Print("Initializing Socket Adapter")
	this.module = module
	this.routeCallback = routeCallback
	handle := &codec.MsgpackHandle{}
	handle.RawToString = true
	this.enc = codec.NewEncoder(this.conn, handle)
	this.dec = codec.NewDecoder(this.conn, handle)
	go this.ReceiveWorker()
}

func (this *SocketAdapter) ConnectToManager(managerIf string) {
	parts := strings.Split(managerIf, "://")
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
	this.conn, err = net.Dial(network, parts[1])
	for err != nil {
		fmt.Errorf("Could not connect, %v", err)
		this.conn, err = net.Dial(network, parts[1])
	}
	fmt.Println("Successfully connected to ", managerIf)
	this.connected = true
}
