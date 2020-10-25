package mr

type Task interface {
	TaskName() string
}

type Map struct {
	MapperNum int
	FileName  string
	NReduce int
}

type Reduce struct {
	ReducerNum int
	NMap int
}

type Finish struct {

}

func (m Map) TaskName() string {
	return "Map"
}

func (r Reduce) TaskName() string {
	return "Reduce"
}

func (f Finish) TaskName() string {
	return "Finish"
}