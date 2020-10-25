package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var args interface{}
	var reply interface{}
	call("Master.RegisterWorker", &args, &reply)

	gob.Register(Map{})
	gob.Register(Reduce{})
	gob.Register(Finish{})

Loop:
	for {
		task := requestTask()
		switch v := task.(type) {
		case Map:
			doMap(v, mapf)
		case Reduce:
			doReduce(v, reducef)
		case Finish:
			break Loop
		}

		reportTaskDone(task)
	}
}

func requestTask() Task {
	var reply TaskReply
	var args TaskArgs
	success := call("Master.GetTask", &args, &reply)
	if (!success) {
		return Finish{}
	}

	return reply.Task
}

func doMap(m Map, mapf func(string, string) []KeyValue) {
	fn := m.FileName
	f, e := ioutil.ReadFile(fn)
	if e != nil {
		log.Fatal("reading:", e)
	}
	kva := mapf(fn, string(f))
	reduces := make([][]KeyValue, m.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % m.NReduce
		reduces[index] = append(reduces[index], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(m.MapperNum, idx)
		f, err := os.Create(fileName)
		if err != nil {
			log.Fatal("opening:", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("writing: ", err)
			}
		}
	}
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

func doReduce(r Reduce, reducef func(string, []string) string) {
	kvMap := make(map[string][]string)
	for i := 0; i < r.NMap; i++ {
		inputName := reduceName(i, r.ReducerNum)
		file, err := os.Open(inputName)
		if err != nil {
			log.Fatalf("failed to read: %v", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		var kv KeyValue
		for err := decoder.Decode(&kv); err == nil; err = decoder.Decode(&kv) {
			key := kv.Key
			val := kv.Value
			kvMap[key] = append(kvMap[key], val)
		}
	}

	var kvList []KeyValue
	for k, v := range kvMap {
		kvList = append(kvList, KeyValue{k, reducef(k, v)})
	}

	outName := mergeName(r.ReducerNum)
	outputFile, err := os.Create(outName)
	if err != nil {
		log.Fatal("Create file:", err)
	}
	for _, kv := range kvList {
		fmt.Fprintf(outputFile, "%v %v\n", kv.Key, kv.Value)
	}
	outputFile.Close()
}

func reportTaskDone(task Task) {
	var reply TaskReply
	var args TaskArgs
	args.Task = task
	call("Master.TaskDone", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
