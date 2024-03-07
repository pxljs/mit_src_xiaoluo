package main

import "C"
import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Arithmetic 实现了RPC接口
type Arithmetic struct {
	A       []string   //用于存放糖果
	B       []string   //用于存放梨子
	Mu      sync.Mutex //添加保护锁
	Request bool
}

type Args struct {
	X int
	Y int
}

type Reply struct {
	Res string
}

// 定义了服务器初始化方法
func (a *Arithmetic) Init() error {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	a.Request = false
	for i := 1; i <= 100; i++ {
		str1 := fmt.Sprint("candy[", i, "]")
		str2 := fmt.Sprint("pear[", i, "]")
		a.A = append(a.A, str1)
		a.B = append(a.B, str2)
	}
	return nil
}
func (a *Arithmetic) Distribute(req *Args, resp *Reply) error {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	a.Request = true
	if len(a.A) > 0 {
		//每次索要X个糖果
		a.A = a.A[:len(a.A)-req.X]
		resp.Res = fmt.Sprint("分发了", req.X, "个糖果")
	} else if len(a.B) > 0 {
		//没有糖果，但还有梨，短暂停留之后索要梨
		fmt.Println("糖果分完了，短暂停留之后索要梨，开始停留...")
		// 停留3秒
		time.Sleep(3 * time.Second)
		fmt.Println("停留结束!")
		a.B = a.B[:len(a.B)-req.Y]
		resp.Res = fmt.Sprint("分发了", req.Y, "个梨")
	} else {
		fmt.Println("糖果和梨均已分完，服务端即将结束!")
		os.Exit(1) //结束服务端进程
	}
	fmt.Printf("当前糖果数量为：%+v,梨的数量为：%+v\n", len(a.A), len(a.B))
	return nil
}

func main() {
	Ser := new(Arithmetic)
	rpc.Register(Ser)
	Ser.Init()
	listener, _ := net.Listen("tcp", ":1234")
	connect, _ := listener.Accept()
	go rpc.ServeConn(connect)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop() // 确保在函数退出前停止Ticker
	for {
		select {
		case <-ticker.C: //计时器到时
			fmt.Println("超过10s未接收到客户端请求，服务器即将关闭")
			return
		default:
			if Ser.Request == true {
				ticker.Reset(10 * time.Second)
				fmt.Println("10s内收到客户端请求，重置ticker")
				Ser.Request = false
				continue
			}
		}
	}
}
