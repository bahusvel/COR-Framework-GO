package main

import (
	"time"
	"./cor"
	"fmt"
)

var req *cor.Module = &cor.Module{}
var res *cor.Module = &cor.Module{}

func ResponseHandle(msg *cor.Message) {
	fmt.Println(msg)
}

func RequestHandle(msg *cor.Message){
	fmt.Println(msg)
	res.MessageOut(cor.Message{Atype: "RESPONSE"})
}

func main() {
	res_routes := map[string]int32 {}
	res_adapter := cor.SocketAdapter{RouteCallback:cor.StaticRouterFactory(&res_routes)}
	res.Init("Response Module", &res_adapter)
	res.AddTopic("REQUEST", RequestHandle)

	req_routes :=map[string]int32 {"REQUEST": res.Mid}

	req_adapter := cor.SocketAdapter{RouteCallback:cor.StaticRouterFactory(&req_routes)}
	req.Init("Request Module", &req_adapter)
	res_routes["RESPONSE"] = req.Mid
	req.AddTopic("RESPONSE", ResponseHandle)

	req_adapter.Connect("tcp://localhost:6050")
	time.Sleep(1 * time.Second)
	for i := 0 ; i < 100 ; i++ {
		req.MessageOut(cor.Message{Atype: "REQUEST"})
	}
	time.Sleep(10 * time.Second)
}
