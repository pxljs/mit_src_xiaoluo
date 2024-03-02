package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)
import "6.824/mr"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	//if len(os.Args) < 3 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
	//	os.Exit(1)
	//}
	// 不从启动命令中读取 pg*.txt 直接指定
	fileNameList := []string{
		"pg-being_ernest.txt",
		"pg-dorian_gray.txt",
		"pg-frankenstein.txt",
		"pg-grimm.txt",
		"pg-huckleberry_finn.txt",
		"pg-metamorphosis.txt",
		"pg-sherlock_holmes.txt",
		"pg-tom_sawyer.txt",
	}
	//mapf, reducef := loadPlugin(os.Args[1])
	// loadPlugin 无法在 Windows 中运行，这里手动 load 我们的 Map 和 Reduce 函数, 在 mrapps 文件夹的 wc.go 中
	mapf := func(filename string, contents string) []mr.KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []mr.KeyValue{}
		for _, w := range words {
			kv := mr.KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}

	// 手动指定 reducef 函数
	reducef := func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range fileNameList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
