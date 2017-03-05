package mapreduce

import (
	"os"
	"encoding/json"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

//type By func(p1, p2 *KeyValue) bool
//func (by By) Sort(keyValue []KeyValue) {
//	ps := &keyValueSorter{
//		keyValue: keyValue,
//		by:      by, // The Sort method's receiver is the function (closure) that defines the sort order.
//	}
//	sort.Sort(ps)
//}
//type keyValueSorter struct {
//	keyValue []KeyValue
//	by      func(p1, p2 *KeyValue) bool // Closure used in the Less method.
//}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	m := make(map[string]([]string))
	for i := 0; i < nMap; i ++ {
		//fmt.Println(reduceName(jobName, i, reduceTaskNumber))
		file, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			panic(err)
		}
		dec := json.NewDecoder(file)
		kv := KeyValue{}
		for err := dec.Decode(&kv); err == nil; err = dec.Decode(&kv) {
			//fmt.Println("key: %s, value: %s", kv.Key, kv.Value)
			vv, ok := m[kv.Key]
			if ok {
				m[kv.Key] = append(vv, kv.Value)
			} else {
				m[kv.Key] = append(make([]string, 0), kv.Value)
			}
		}
		file.Close()
	}
	file, err := os.Create(outFile)
	if err != nil {
		panic(err)
	}
	enc := json.NewEncoder(file)
	for k, v := range m {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
	file.Close()
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
