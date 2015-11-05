package core

import (
	"fmt"
)

/**
 * map: (k1; v1) → [(k2; v2)]
 */
type Mapper func(key interface{}, value interface{}) map[interface{}]interface{}

/**
 * reduce: (k2; [v2]) → [(k3; v3)]
 */
type Reducer func(key interface{}, values []interface{}) map[interface{}]interface{}

/**
 * aggregate: (k1,v1);(k1,v2);(k2,v3).... → k1,[v1,v2];k2,[v3];....
 */
func Aggregate(values []map[interface{}]interface{}) map[interface{}][]interface{} {
	result := make(map[interface{}][]interface{})
	for _, value := range values {
		for k, v := range value {
			_, ok := result[k]
			if ok {
				result[k] = append(result[k], v)
			} else {
				result[k] = []interface{}{v}
			}
		}
	}

	return result
}

func Reduce(in map[interface{}][]interface{}, reducer Reducer) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	for k, v := range in {
		reduceResult := reducer(k, v)
		for k2, v2 := range reduceResult {
			ret[k2] = v2
		}
	}

	return ret
}

func PrintMap(m map[interface{}]interface{}) {
	for k, v := range m {
		println(fmt.Sprintf("%v          %v", k, v))
	}
}
