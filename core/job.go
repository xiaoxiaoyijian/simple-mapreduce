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
		MapperChanLen:  100,
		UnitsPerMapper: 100,
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

func (this *Job) Reduce(in chan map[interface{}]interface{}) map[interface{}]interface{} {
	return Reduce(Aggregate(in), this.reducer)
}
