package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func callProcess(i int, jobName string, filename string, n_other int, phase jobPhase, registerChan chan string, done []chan bool) {
	args := DoTaskArgs{jobName, filename, phase, i, n_other}
	a := <- registerChan
	//fmt.Printf("%v\n", a)
	ok := call(a, "Worker.DoTask", args, nil)
	for ok == false {
		fmt.Printf("retry #%d %v\n", i, phase)
		a = <- registerChan
		ok = call(a, "Worker.DoTask", args, nil)
	}
	done[i] <- true
	registerChan <- a
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	done := make([]chan bool, ntasks)
	for i := range done {
		done[i] = make(chan bool, 2)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	if phase == mapPhase {
		for i := 0; i < ntasks; i ++ {
			go callProcess(i, jobName, mapFiles[i], n_other, phase, registerChan, done)
		}
	} else {
		for i := 0; i < ntasks; i ++ {
			go callProcess(i, jobName, "", n_other, phase, registerChan, done)
		}
	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	for index := range done {
		fmt.Printf("wait %d\n", index)
		<- done[index]
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
