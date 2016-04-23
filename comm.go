package cor

import (
	"fmt"
	"net"
	"strings"
	"encoding/binary"
	"github.com/bahusvel/COR-Framework-GO/protocol"
	"github.com/golang/protobuf/proto"
)


type NetworkAdapter interface {
	Init(module *Module)
	MessageOut(msg *interface{})
}

type SocketAdapter struct {
	module     	  *Module
	protocol	  string
	routes		map[string] string
	sockets		map[string]	net.Conn
}

func (this *SocketAdapter) MessageOut(msg *interface{}) error{
	msgType := getType(msg)
	route, ok := this.routes[msgType]
	if !ok {
		fmt.Println("Don't know where to send ", getType(msgType))
		return error("Don't know where to send " + getType(msgType))
	}
	if sock, ok := this.sockets[route]; ok{
		cormsg := cor.CORMessage{}
		cormsg.Type = msgType
		data, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		cormsg.Data = data
		cordata, err := proto.Marshal(cormsg)
		if err != nil {
			return err
		}
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, len(cordata))
		sock.Write(length)
		sock.Write(cordata)
	}
	return nil
}

func (this *SocketAdapter) connectionHandler(conn net.Conn){
	length := make([]byte, 4)
	for {
		bread, err := conn.Read(length)
		if bread != 4 || err != nil {

		}
		cormsg := cor.CORMessage{}
		proto.Unmarshal()
		msg := Message{}
		msg.FromMap(msgmap)
		if _, ok := this.encoderMap[msg.Source[0]]; !ok {
			delete(this.connectionMap, conn)
			this.encoderMap[msg.Source[0]] = enc
		}
		this.module.messageIn(&msg)
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
