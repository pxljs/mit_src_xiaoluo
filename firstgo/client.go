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
	if err != nil {
		log.Fatal("dialing error", err)
	}
	arg := Args{X: 2, Y: 1}
	reply := Reply{}
	err = client.Call("Arithmetic.Distribute", arg, &reply)
	if err != nil {
		log.Fatal("RPC error", err)
	}
	log.Println(reply)
}
