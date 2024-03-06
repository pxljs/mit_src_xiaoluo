package main

import (
	"fmt"
	"sync"
)

// mergeChannels 接收多个通道，并返回一个合并后的通道
func mergeChannels(channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	// 创建一个合并后的通道
	merged := make(chan int)

	// 为每个输入通道启动一个协程
	outputChannels := make([]chan int, len(channels))
	for i, c := range channels {
		outputChannels[i] = make(chan int)
		wg.Add(1)
		go func(ch <-chan int, out chan<- int) {
			defer wg.Done()
			for num := range ch {
				out <- num // 将通道中的值发送到输出通道
			}
			close(out) // 发送完数据后关闭输出通道
		}(c, outputChannels[i])
	}

	// 合并所有输出通道到merged通道
	go func() {
		wg.Wait()     // 等待所有协程完成
		close(merged) // 所有协程完成后关闭merged通道
	}()

	go func() {
		for _, out := range outputChannels {
			for num := range out {
				merged <- num // 从输出通道中读取并发送到merged通道
			}
		}
	}()

	return merged
}

func main() {
	// 创建多个切片
	slice1 := []int{1, 3, 5, 7}
	slice2 := []int{2, 4, 6, 8}
	slice3 := []int{9, 11, 13, 15}

	// 将切片转换为通道
	channel1 := make(chan int, len(slice1))
	channel2 := make(chan int, len(slice2))
	channel3 := make(chan int, len(slice3))

	// 将切片中的值发送到通道
	for _, num := range slice1 {
		channel1 <- num
	}
	for _, num := range slice2 {
		channel2 <- num
	}
	for _, num := range slice3 {
		channel3 <- num
	}

	// 合并通道
	merged := mergeChannels(channel1, channel2, channel3)

	// 从合并后的通道中读取并打印值
	for num := range merged {
		fmt.Println(num)
	}
}
