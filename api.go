package main

import (
	"math/rand"
	"time"
	"fmt"
	codec "github.com/ugorji/go/codec"
	"net"
	"strings"
)

func IdGenerator() int32{
	rand.Seed(int64(time.Now().Unix()))
	return rand.Int31()
}

type Module struct {
	mid int32
	produces []string
	consumes map[string] func(*Message)
	connected bool
	conn net.Conn
	enc *codec.Encoder
	dec *codec.Decoder
}

func (this *Module) ReceiveWorker(){
	for {
		msgmap := make(map[string] interface{})
		this.dec.Decode(&msgmap)
		msg := Message{}
		msg.FromMap(msgmap)
		if handler, ok := this.consumes[msg.atype]; ok == true {
			handler(&msg)
		}
	}
}

func (this *Module) Init(args map[string] interface{}){
	if val, ok := args["mid"] ; ok == true{
		this.mid = int32(val.(int))
	} else {
		this.mid = IdGenerator()
	}
	this.consumes = make(map[string] func(*Message))
	this.ConnectToManager("tcp://127.0.0.1:8888")
	handle := &codec.MsgpackHandle{}
	handle.RawToString = true
	this.enc = codec.NewEncoder(this.conn, handle)
	this.dec = codec.NewDecoder(this.conn, handle)
	go this.ReceiveWorker()
	fmt.Println("Initializing %s %s")
}

func (this *Module) AddTopic(topic string, callback func (*Message)){
	this.consumes[topic] = callback
	keys := make([]string, 0, len(this.consumes))
	for i := range this.consumes{
		keys = append(keys, i)
	}
	ta := Message{atype: "TOPIC_ADVERTISEMENT", payload: map[string] interface{}{"consumes": keys}}
	this.MessageOut(ta)
}

func (this *Module) MessageOut(msg Message){
	msg.source = append(msg.source, this.mid)
	msg.number = IdGenerator()
	this.enc.Encode(msg.ToMap())
}

func (this *Module) ConnectToManager(managerIf string){
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
	this.conn, err = net.Dial(network, parts[1]);
	for err != nil {
		fmt.Errorf("Could not connect, %v", err)
		this.conn, err = net.Dial(network, parts[1]);
	}
	fmt.Println("Successfully connected to ", managerIf)
	this.connected = true
}

type Message struct {
	source []int32
	destination []int32
	atype string
	number int32
	payload interface{}
}

func (this *Message) FromMap(mm map[string] interface{}) {
	this.number = int32(mm["number"].(uint64))
	this.atype = mm["atype"].(string)
	this.destination = ai2aint(mm["destination"].([]interface{}))
	this.payload = mm["payload"]
	this.source = ai2aint(mm["source"].([]interface{}))
}

func (this *Message) ToMap() map[string] interface{}{
	tmp := make(map[string] interface{})
	tmp["source"] = this.source
	tmp["destination"] = this.destination
	tmp["atype"] = this.atype
	tmp["number"] = this.number
	tmp["payload"] = this.payload
	return tmp
}

func ai2aint(input []interface{}) []int32{
	output := make([]int32, 0, len(input))
	for _, i := range input{
		output = append(output, int32(i.(uint64)))
	}
	return output
}

func ResponseHandle(msg *Message) {
	fmt.Println(msg)
}

func main() {
	zm := Module{}
	zm.Init(map[string]interface{} {})
	zm.AddTopic("RESPONSE", ResponseHandle)
	zm.MessageOut(Message{atype: "HelloWorld"})
	time.Sleep(10 * time.Second)
}
