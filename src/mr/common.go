package mr

import (
	"fmt"
	"log"
)

// Debugging enabled?
const Debug = false

func DPrintf(format string, a ... interface{}) {
	if Debug {
		log.Printf(format + "\n", a ...)
	}
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int
	Phase    TaskPhase
	Alive    bool // worker should exit when alive is false
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

