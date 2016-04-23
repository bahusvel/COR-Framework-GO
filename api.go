package cor

import (
	"fmt"
	"reflect"
	"strings"
)


type Module struct {
	modulename		string
	consumes		map[string] func(*interface{})
	types 			map[string] interface{}
	networkAdapter NetworkAdapter
}

func (this *Module) Init(modulename string, networkAdapter NetworkAdapter){
	this.networkAdapter = networkAdapter
	this.networkAdapter.Init(this)
	this.modulename = modulename
	this.consumes = make(map[string] func(*interface{}))
	this.types = make(map[string] interface{})
	fmt.Println("Initializing", modulename)
}

func (this *Module) AddTopic(topic string, callback func (*interface{})){
	this.consumes[topic] = callback
}

func (this *Module) AddType(messageType string, prototype interface{}){
	typeProto := getType(prototype)
	if typeProto != messageType{
		panic(fmt.Sprintf("Message Type: %s does not match the type of prototype: %s", messageType, prototype))
	}
	this.types[messageType] = prototype
}

func getType(something interface{}) string{
	parts := strings.Split(reflect.TypeOf(something).String(), ".")
	return parts[len(parts) - 1]
}

func (this *Module) messageIn(msg *interface{}){
	if handler, ok := this.consumes[getType(msg)]; ok {
		handler(msg)
	}
}

func (this *Module) MessageOut(msg *interface{}){
	this.networkAdapter.MessageOut(msg)
}