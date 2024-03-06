package main

import (
	"log"
	"net/rpc"
)

type Args struct {
	X int
	Y int
}

type Reply struct {
	Res string
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")
	Request := make(chan bool)
	if err != nil {
		log.Fatal("dialing error", err)
	}
	arg := Args{X: 2, Y: 1}
	reply := Reply{}
	client.Call("Arithmetic.Distribute", arg, &reply)
	Request <- true
	log.Println(reply)
}
