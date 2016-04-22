package cor

import (
	"math/rand"
	"time"
	"fmt"
)


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

func (this *Module) AddTyoe(topic string, interface{}){

}

func (this *Module) messageIn(msg *Message){
	if handler, ok := this.consumes[msg.Atype]; ok {
			handler(msg)
	}
}

func (this *Module) MessageOut(msg Message){
	msg.Source = append(msg.Source, this.Mid)
	msg.Destination = []int32 {}
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
