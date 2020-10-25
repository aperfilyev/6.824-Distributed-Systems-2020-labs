package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Master definition
type Master struct {
	nMap       int
	nReduce    int
	nWorkers   int
	tasks      chan Task
	inProgress map[Task]int64

	mux sync.Mutex
	wg  sync.WaitGroup

	stopTicker chan bool
	done       bool
}

func (m *Master) RegisterWorker(args *interface{}, reply *interface{}) error {
	m.mux.Lock()
	m.nWorkers++
	m.mux.Unlock()
	return nil
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	for {
		select {
		case t := <-m.tasks:
			reply.Task = t
			m.mux.Lock()
			m.inProgress[t] = time.Now().Unix()
			m.mux.Unlock()
			return nil
		}
	}
}

func (m *Master) TaskDone(args *TaskArgs, reply *TaskReply) error {
	m.mux.Lock()
	delete(m.inProgress, args.Task)
	m.mux.Unlock()
	m.wg.Done()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	gob.Register(Map{})
	gob.Register(Reduce{})
	gob.Register(Finish{})

	m := Master{
		nMap:       len(files),
		nReduce:    nReduce,
		tasks:      make(chan Task),
		inProgress: make(map[Task]int64),

		stopTicker: make(chan bool),
	}

	m.enqueueTasks(files, nReduce)

	m.startTicker()

	m.server()

	return &m
}

func (m *Master) enqueueTasks(files []string, nReduce int) {
	go func() {
		nMap := len(files)
		for i, file := range files {
			m.tasks <- Map{i, file, nReduce}
			m.wg.Add(1)
		}

		m.wg.Wait()

		for i := 0; i < nReduce; i++ {
			m.tasks <- Reduce{i, nMap}
			m.wg.Add(1)
		}

		m.wg.Wait()

		m.mux.Lock()
		workers := m.nWorkers
		m.mux.Unlock()
		for i := 0; i < workers; i++ {
			m.tasks <- Finish{}
		}
		m.stopTicker <- true
	}()
}

func (m *Master) startTicker() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-m.stopTicker:
				ticker.Stop()
				m.mux.Lock()
				m.done = true
				m.mux.Unlock()
				return
			case <-ticker.C:
				now := time.Now().Unix()
				m.mux.Lock()
				for k, v := range m.inProgress {
					if now-v >= 10 {
						m.tasks <- k
						delete(m.inProgress, k)
					}
				}
				m.mux.Unlock()
			}
		}
	}()
}
