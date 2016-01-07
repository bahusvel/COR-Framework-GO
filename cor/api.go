package cor

import (
	"math/rand"
	"time"
	"fmt"
)

type Message struct {
	Source      []int32
	Destination []int32
	Atype       string
	Number      int32
	Payload     interface{}
}

func (this *Message) FromMap(mm map[string] interface{}) {
	this.Number = int32(mm["number"].(uint64))
	this.Atype = mm["atype"].(string)
	this.Destination = ai2aint(mm["destination"].([]interface{}))
	this.Payload = mm["payload"]
	this.Source = ai2aint(mm["source"].([]interface{}))
}

func (this *Message) ToMap() map[string] interface{}{
	tmp := make(map[string] interface{})
	tmp["source"] = this.Source
	tmp["destination"] = this.Destination
	tmp["atype"] = this.Atype
	tmp["number"] = this.Number
	tmp["payload"] = this.Payload
	return tmp
}

func IdGenerator() int32{
	rand.Seed(int64(time.Now().Unix()))
	return rand.Int31()
}

type Module struct {
	Mid            int32
	modulename     string
	produces       []string
	consumes       map[string] func(*Message)
	networkAdapter NetworkAdapter
}

func (this *Module) Init(modulename string, networkAdapter NetworkAdapter){
	this.Mid = IdGenerator()
	this.networkAdapter = networkAdapter
	this.networkAdapter.Init(this)
	this.modulename = modulename
	this.consumes = make(map[string] func(*Message))
	fmt.Println("Initializing", modulename, this.Mid)
}

func (this *Module) AddTopic(topic string, callback func (*Message)){
	this.consumes[topic] = callback
	this.topicAdvertisement()
}

func (this *Module) topicAdvertisement(){
	keys := make([]string, 0, len(this.consumes))
	for i := range this.consumes{
		keys = append(keys, i)
	}
	ta := Message{Atype: "TOPIC_ADVERTISEMENT", Payload: map[string] interface{}{"consumes": keys}}
	this.MessageOut(ta)
}

func (this *Module) MessageOut(msg Message){
	msg.Source = append(msg.Source, this.Mid)
	msg.Number = IdGenerator()
	this.networkAdapter.MessageOut(&msg)
}

func ai2aint(input []interface{}) []int32{
	output := make([]int32, 0, len(input))
	for _, i := range input{
		output = append(output, int32(i.(uint64)))
	}
	return output
}
