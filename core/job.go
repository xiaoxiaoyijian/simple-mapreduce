package core

import (
	"sync"
)

type Job struct {
	mapper  Mapper
	reducer Reducer

	MapperChanLen  int
	UnitsPerMapper int
}

func NewJob(m Mapper, r Reducer) *Job {
	return &Job{
		mapper:         m,
		reducer:        r,
		MapperChanLen:  10,
		UnitsPerMapper: 10,
	}
}

func (this *Job) Run(in chan interface{}) map[interface{}]interface{} {
	return this.Reduce(this.Map(in))
}

func (this *Job) Map(in_chan chan interface{}) chan map[interface{}]interface{} {
	out_chan := make(chan map[interface{}]interface{}, this.MapperChanLen)

	go func() {
		defer close(out_chan)

		units := []interface{}{}
		var wg sync.WaitGroup
		for v := range in_chan {
			if len(units) >= this.UnitsPerMapper {
				wg.Add(1)
				go func() {
					defer wg.Done()

					out_chan <- this.mapper("", units)
				}()

				units = []interface{}{}
			}

			units = append(units, v)
		}

		if len(units) > 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				out_chan <- this.mapper("", units)
			}()
		}

		wg.Wait()
	}()

	return out_chan
}

func (this *Job) Reduce(in_chan chan map[interface{}]interface{}) map[interface{}]interface{} {
	mapResult := []map[interface{}]interface{}{}
	for v := range in_chan {
		mapResult = append(mapResult, v)
	}

	return Reduce(Aggregate(mapResult), this.reducer)
}
