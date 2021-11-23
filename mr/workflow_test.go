package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tal-tech/go-zero/core/stringx"
	"github.com/tal-tech/go-zero/core/syncx"
	"fmt"
)

var errDummy = errors.New("dummy")

func init() {
	log.SetOutput(ioutil.Discard)
}

type RuleA struct {
	Request  interface{}
	State    int //0 串行 1并行
	Priority uint32
}

type RuleCommon struct {
	State    int //0 串行 1并行
	Priority uint32
}
type RuleB struct {
	State    int //0 串行 1并行
	Priority uint32
}
type RuleC struct {
	State    int //0 串行 1并行
	Priority uint32
}
type RuleD struct {
	State    int //0 串行 1并行
	Priority uint32
}
type RuleE struct {
	State    int //0 串行 1并行
	Priority uint32
}

//get 出当前规则是串行还是并行。
func (a RuleA) Get() int {
	return a.State
}

//规则A 确定要不要处理
func (a RuleA) Output(req interface{}) *RuleOutput {
	// if statu=1 and url!=""
	fmt.Println("A had call,I should sleep 3s")
	time.Sleep(3 * time.Second)
	return &RuleOutput{
		Status:   0, //0   1
		Result:   "business A resp",
		Priority: a.Priority,
	}
}

//规则B 确定要不要处理
func (a RuleB) Output(req interface{}) *RuleOutput {
	fmt.Println("B had call,I should sleep 1s")
	time.Sleep(1 * time.Second)
	return &RuleOutput{
		Status:   1,
		Result:   "business B resp",
		Priority: a.Priority,
	}
}
func (a RuleC) Output(req interface{}) *RuleOutput {
	fmt.Println("C had call,I should sleep 2s")
	time.Sleep(2 * time.Second)
	return &RuleOutput{
		Status:   0,
		Result:   "business C resp",
		Priority: a.Priority,
	}
}

func (a RuleE) Output(req interface{}) *RuleOutput {
	fmt.Println("E had call,I should sleep 1s")
	time.Sleep(1 * time.Second)
	return &RuleOutput{
		Status:   0,
		Result:   "business E res",
		Priority: a.Priority,
	}
}

// URulePro's sceneID mapping ruleFlow
// 缺点...固定好的编排
var URulePro = map[int][]interface{}{
	1: {
		RuleE{State: Serial},
		RuleA{State: Parallel},
		RuleB{State: Parallel},
		RuleC{State: Serial},
		RuleD{State: Serial},
	},
}

func TestFinish(t *testing.T) {
	//v4---抽离串行、并行、优先级并行
	//r := WorkFlowManager([]interface{}{
	//	WGroup{
	//		Type: SerialType,
	//		Rules:
	//		[]interface{}{
	//			RuleE{State: Serial},
	//		},},
	//	WGroup{
	//		Type: ParallelType,
	//		Rules: []interface{}{
	//			RuleA{State: Parallel, Priority: 2},
	//			RuleB{State: Parallel, Priority: 8},
	//			RuleC{State: Parallel, Priority: 4},
	//		},},
	//	WGroup{
	//		Type: PriorityParallelType,
	//		Rules: []interface{}{
	//			RuleA{State: Parallel, Priority: 2},
	//			RuleB{State: Parallel, Priority: 4},
	//			RuleC{State: Parallel, Priority: 8},
	//		},},
	//})
	r2 := WorkFlowManager([]interface{}{
		//WGroup{
		//	Type: SerialType,
		//	Rules:
		//	[]interface{}{
		//		RuleE{State: Serial},
		//	},},
		WGroup{
			Type: PriorityParallelType,
			Rules: []interface{}{
				RuleA{State: Parallel, Priority: 2},
				RuleB{State: Parallel, Priority: 4},
				RuleC{State: Parallel, Priority: 8},
			},},
	})
	fmt.Println(r2)

	return

}

func TestFinishNone(t *testing.T) {
	assert.Nil(t, Finish())
}

func TestFinishVoidNone(t *testing.T) {
	FinishVoid()
}

func TestFinishErr(t *testing.T) {
	var total uint32
	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		//return errDummy
		return errors.New("11")
		return nil

	}, func() error {
		atomic.AddUint32(&total, 5)
		return errors.New("22")
	})

	fmt.Println(total)
	assert.Equal(t, errDummy, err)
}

func TestFinishVoid(t *testing.T) {
	var total uint32
	FinishVoid(func() {
		atomic.AddUint32(&total, 2)
		fmt.Println("哈哈哈")
		time.Sleep(1 * time.Second)
	}, func() {
		atomic.AddUint32(&total, 3)
		fmt.Println("哈哈哈2")
		time.Sleep(10 * time.Second)
	}, func() {
		atomic.AddUint32(&total, 5)
		fmt.Println("哈哈哈")
		time.Sleep(1 * time.Second)
	})
	fmt.Println(total)
	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
}

func TestMap(t *testing.T) {
	tests := []struct {
		mapper MapFunc
		expect int
	}{
		{
			mapper: func(item interface{}, writer Writer) {
				v := item.(int)
				writer.Write(v * v)
			},
			expect: 30,
		},
		{
			mapper: func(item interface{}, writer Writer) {
				v := item.(int)
				if v%2 == 0 {
					return
				}
				writer.Write(v * v)
			},
			expect: 10,
		},
		{
			mapper: func(item interface{}, writer Writer) {
				v := item.(int)
				if v%2 == 0 {
					panic(v)
				}
				writer.Write(v * v)
			},
			expect: 10,
		},
	}

	for _, test := range tests {
		t.Run(stringx.Rand(), func(t *testing.T) {
			channel := Map(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, WithWorkers(-1))

			var result int
			for v := range channel {
				result += v.(int)
			}

			assert.Equal(t, test.expect, result)
		})
	}
}

func TestMapReduce(t *testing.T) {
	tests := []struct {
		mapper      MapperFunc
		reducer     ReducerFunc
		expectErr   error
		expectValue interface{}
	}{
		{
			expectErr:   nil,
			expectValue: 30,
		},
		{
			mapper: func(item interface{}, writer Writer, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			mapper: func(item interface{}, writer Writer, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr:   ErrCancelWithNil,
			expectValue: nil,
		},
		{
			reducer: func(pipe <-chan interface{}, writer Writer, cancel func(error)) {
				var result int
				for item := range pipe {
					result += item.(int)
					if result > 10 {
						cancel(errDummy)
					}
				}
				writer.Write(result)
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(stringx.Rand(), func(t *testing.T) {
			if test.mapper == nil {
				test.mapper = func(item interface{}, writer Writer, cancel func(error)) {
					v := item.(int)
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan interface{}, writer Writer, cancel func(error)) {
					var result int
					for item := range pipe {
						result += item.(int)
					}
					writer.Write(result)
				}
			}
			value, err := MapReduce(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer, WithWorkers(runtime.NumCPU()))

			assert.Equal(t, test.expectErr, err)
			assert.Equal(t, test.expectValue, value)
		})
	}
}

func TestMapReduceVoid(t *testing.T) {
	var value uint32
	tests := []struct {
		mapper      MapperFunc
		reducer     VoidReducerFunc
		expectValue uint32
		expectErr   error
	}{
		{
			expectValue: 30,
			expectErr:   nil,
		},
		{
			mapper: func(item interface{}, writer Writer, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			mapper: func(item interface{}, writer Writer, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr: ErrCancelWithNil,
		},
		{
			reducer: func(pipe <-chan interface{}, cancel func(error)) {
				for item := range pipe {
					result := atomic.AddUint32(&value, uint32(item.(int)))
					if result > 10 {
						cancel(errDummy)
					}
				}
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(stringx.Rand(), func(t *testing.T) {
			atomic.StoreUint32(&value, 0)

			if test.mapper == nil {
				test.mapper = func(item interface{}, writer Writer, cancel func(error)) {
					v := item.(int)
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan interface{}, cancel func(error)) {
					for item := range pipe {
						atomic.AddUint32(&value, uint32(item.(int)))
					}
				}
			}
			err := MapReduceVoid(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer)

			assert.Equal(t, test.expectErr, err)
			if err == nil {
				assert.Equal(t, test.expectValue, atomic.LoadUint32(&value))
			}
		})
	}
}

func TestMapReduceVoidWithDelay(t *testing.T) {
	var result []int
	err := MapReduceVoid(func(source chan<- interface{}) {
		source <- 0
		source <- 1
	}, func(item interface{}, writer Writer, cancel func(error)) {
		i := item.(int)
		if i == 0 {
			time.Sleep(time.Millisecond * 50)
		}
		writer.Write(i)
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			i := item.(int)
			result = append(result, i)
		}
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, 1, result[0])
	assert.Equal(t, 0, result[1])
}

func TestMapVoid(t *testing.T) {
	const tasks = 1000
	var count uint32
	MapVoid(func(source chan<- interface{}) {
		for i := 0; i < tasks; i++ {
			source <- i
		}
	}, func(item interface{}) {
		atomic.AddUint32(&count, 1)
	})

	assert.Equal(t, tasks, int(count))
}

func TestMapReducePanic(t *testing.T) {
	v, err := MapReduce(func(source chan<- interface{}) {
		source <- 0
		source <- 1
	}, func(item interface{}, writer Writer, cancel func(error)) {
		i := item.(int)
		writer.Write(i)
	}, func(pipe <-chan interface{}, writer Writer, cancel func(error)) {
		for range pipe {
			panic("panic")
		}
	})
	assert.Nil(t, v)
	assert.NotNil(t, err)
	assert.Equal(t, "panic", err.Error())
}

func TestMapReduceVoidCancel(t *testing.T) {
	var result []int
	err := MapReduceVoid(func(source chan<- interface{}) {
		source <- 0
		source <- 1
	}, func(item interface{}, writer Writer, cancel func(error)) {
		i := item.(int)
		if i == 1 {
			cancel(errors.New("anything"))
		}
		writer.Write(i)
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			i := item.(int)
			result = append(result, i)
		}
	})
	assert.NotNil(t, err)
	assert.Equal(t, "anything", err.Error())
}

func TestMapReduceVoidCancelWithRemains(t *testing.T) {
	var done syncx.AtomicBool
	var result []int
	err := MapReduceVoid(func(source chan<- interface{}) {
		for i := 0; i < defaultWorkers*2; i++ {
			source <- i
		}
		done.Set(true)
	}, func(item interface{}, writer Writer, cancel func(error)) {
		i := item.(int)
		if i == defaultWorkers/2 {
			cancel(errors.New("anything"))
		}
		writer.Write(i)
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			i := item.(int)
			result = append(result, i)
		}
	})
	assert.NotNil(t, err)
	assert.Equal(t, "anything", err.Error())
	assert.True(t, done.True())
}

func BenchmarkMapReduce(b *testing.B) {
	b.ReportAllocs()

	mapper := func(v interface{}, writer Writer, cancel func(error)) {
		writer.Write(v.(int64) * v.(int64))
	}
	reducer := func(input <-chan interface{}, writer Writer, cancel func(error)) {
		var result int64
		for v := range input {
			result += v.(int64)
		}
		writer.Write(result)
	}

	for i := 0; i < b.N; i++ {
		MapReduce(func(input chan<- interface{}) {
			for j := 0; j < 2; j++ {
				input <- int64(j)
			}
		}, mapper, reducer)
	}
}
