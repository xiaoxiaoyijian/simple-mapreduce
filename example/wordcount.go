package main

import (
	"github.com/xiaoxiaoyijian/simple-mapreduce/core"
	"github.com/xiaoxiaoyijian/simple-mapreduce/utils/file"
	"math/rand"
	"runtime"
	"strings"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(int64(time.Second))

	job := core.NewJob(dirMapper, wordReducer)
	result := job.Run(file.ReadDir("./", ""))
	core.PrintMap(result)
}

func dirMapper(key interface{}, value interface{}) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})

	files, ok := value.([]interface{})
	if !ok {
		return ret
	}

	job := core.NewJob(lineMapper, wordReducer)
	jobResults := []map[interface{}]interface{}{}
	for _, v := range files {
		filename := strings.TrimSpace(v.(string))
		jobResults = append(jobResults, job.Run(file.ReadLines(filename)))
	}

	return core.Reduce(core.Aggregate(jobResults), wordReducer)
}

func lineMapper(key interface{}, value interface{}) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})

	lines, ok := value.([]interface{})
	if !ok {
		return ret
	}

	for _, v := range lines {
		line := strings.TrimSpace(v.(string))
		tokens := strings.Split(line, " ")
		for _, value := range tokens {
			if value != "" {
				v, ok := ret[value]
				if ok {
					ret[value] = v.(int) + 1
				} else {
					ret[value] = 1
				}
			}
		}
	}

	return ret
}

func wordReducer(key interface{}, values []interface{}) map[interface{}]interface{} {
	cnt := 0
	for _, value := range values {
		if v, ok := value.(int); ok {
			cnt += v
		}
	}

	return map[interface{}]interface{}{
		key: cnt,
	}
}
