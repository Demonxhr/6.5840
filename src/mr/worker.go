package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapWorker(mapf func(string, string) []KeyValue, answer *Task) {
	intermediate := []KeyValue{}
	file, err := os.Open(answer.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", answer.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", answer.FileName)
	}
	file.Close()
	kva := mapf(answer.FileName, string(content))
	intermediate = append(intermediate, kva...)
	outFiles := make([]*os.File, answer.NReduce)
	fileEncs := make([]*json.Encoder, answer.NReduce)
	for outindex := 0; outindex < answer.NReduce; outindex++ {
		outFiles[outindex], _ = ioutil.TempFile("../mr-tmp", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % answer.NReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", answer.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	outprefix := "mr-" + strconv.Itoa(answer.TaskId) + "-"
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		os.Rename(oldpath, outname)
		file.Close()
	}
	args := RequestArgs{}
	args.RequestNum = 1
	args.TaskId = answer.TaskId
	reply := Task{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(1)
	}

}
func reduceWorker(reducef func(string, []string) string, answer *Task) {
	prefix := "mr-"
	lastfix := "-" + strconv.Itoa(answer.TaskId)
	intermediate := []KeyValue{}
	for index := 0; index < answer.NMap; index++ {
		path := prefix + strconv.Itoa(index) + lastfix
		file, err := os.Open(path)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", path, err)
			panic("Open file error")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile("../mr-tmp", "mr-*")
	outname := "mr-out-" + strconv.Itoa(answer.TaskId)
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outname)
	fmt.Println(outname)
	ofile.Close()
	args := RequestArgs{}
	args.RequestNum = 2
	args.TaskId = answer.TaskId
	reply := Task{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(1)
	}

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		reply := CallExample()
		if reply.TaskType == 0 { // map任务
			mapWorker(mapf, reply)
		} else if reply.TaskType == 1 { // reduce任务
			reduceWorker(reducef, reply)
		} else if reply.TaskType == 2 { // 无任务
			time.Sleep(time.Second * 3)
		} else if reply.TaskType == 3 { // 所有任务完成
			os.Exit(1)
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() (r *Task) {

	// declare an argument structure.
	args := RequestArgs{}

	// fill in the argument(s).
	//args.X = 99
	args.RequestNum = 0
	args.TaskId = 0
	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		return &reply
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		os.Exit(1)
		return nil
		//fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
