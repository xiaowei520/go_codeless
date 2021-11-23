package mr

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tal-tech/go-zero/core/errorx"
	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/syncx"
	"github.com/tal-tech/go-zero/core/threading"
	"reflect"
	"sync/atomic"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var ErrCancelWithNil = errors.New("mapreduce cancelled with nil")

type (
	GenerateFunc func(source chan<- interface{})
	MapFunc func(item interface{}, writer Writer)
	VoidMapFunc func(item interface{})
	MapperFunc func(item interface{}, writer Writer, cancel func(error))
	ReducerFunc func(pipe <-chan interface{}, writer Writer, cancel func(error))
	VoidReducerFunc func(pipe <-chan interface{}, cancel func(error))
	Option func(opts *mapReduceOptions)

	mapReduceOptions struct {
		workers int
	}

	Writer interface {
		Write(v interface{})
	}
)

// RuleOutput
type RuleOutput struct {
	Status int
	// Priority. build
	Priority uint32
	Result   interface{}
	Error    error
}

// 场景识别
type IScene interface {
	Get(interface{}) *SceneOutput
}

// BaseScene
type BaseScene struct {
}

// get base scene info
func (a *BaseScene) Get(req interface{}) *SceneOutput {
	//if... scene 1
	return &SceneOutput{
		SceneId: 111,
		Request: "biz resp",
	}
}

// OtherScene
type OtherScene struct {
}

// SceneOutput  is scene's info
type SceneOutput struct {
	SceneId int
	//...规则引擎
	//串行A 串行B 并行C 并行D 并行E 串行F 串行G ---  map配置
	Request interface{}
	Result  interface{}
}

const (
	Serial   = 0 //串行
	Parallel = 1 //并行

	RuleErrorStatus = 1 //1 rule rpc fail or biz fail
)

type IRule interface {
	Output(req interface{}) *RuleOutput
}
type RuleBase struct{}

const (
	PriorityParallelType = iota + 1
	ParallelType
	SerialType
)

type WGroup struct {
	//type
	// 0 is auto type. Only simple serial and parallel is performed
	// 1 is priority Parallel type. So I need check Rules priority value.
	// 2 is Parallel type.
	// 3 is auto Serial type

	Type int

	//Rules
	//example:     RuleA{State: Parallel, Priority: 1},
	Rules []interface{}

	//Hub
	//0 if len([]*RuleOutput) not zero  then return
	//1
	Hub int
}

// WorkFlow interface
type IWorkFlow interface {
	DoSerialGroup() (res []RuleOutput)
	DoPriorityGroup() (res []RuleOutput)
	DoParallelGroup() (res []RuleOutput)
}

// group DoSerialGroup
func (w *WGroup) DoSerialGroup() (res []RuleOutput) {
	for ruleIndex := 0; ruleIndex < len(w.Rules); ruleIndex++ {
		rule := w.Rules[ruleIndex]
		state := reflect.ValueOf(rule).FieldByName("State").Int()
		//Serial do
		if state == Serial {
			sOutput := DoRule(rule)
			if sOutput.Status == RuleErrorStatus {
				fmt.Println(sOutput)
				res = append(res, *sOutput)
				return
			}
		} else {
			//error
		}
	}
	return
}

type PriorityGroup struct {
	sum      uint32
	iterator uint32
	move     uint32
	Optimal  RuleOutput
}

//2^(n-1) + 2^(n-2) ...n=len()
//to be optimized   two bit  10&10
func (p *PriorityGroup) Do(length uint32, sro *RuleOutput) {
	atomic.AddUint32(&p.sum, sro.Priority)
	atomic.AddUint32(&p.move, 2<<(length-atomic.AddUint32(&p.iterator, 1)))
	if sro.Status == RuleErrorStatus && p.Optimal.Priority < sro.Priority {
		p.Optimal = *sro
	}
}
func (p *PriorityGroup) IsHit() (bool, *RuleOutput) {
	if p.sum >= p.move && p.Optimal.Status == RuleErrorStatus {
		return true, &p.Optimal
	}
	return false, nil
}

// DoPriorityGroup
func (w *WGroup) DoPriorityGroup() (res []RuleOutput) {
	fns := []interface{}{}
	lenRule := len(w.Rules)
	for ruleIndex := 0; ruleIndex < lenRule; ruleIndex++ {
		fns = append(fns, w.Rules[ruleIndex])
	}
	priorityGroup := PriorityGroup{}
	MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		sOutput := DoRule(item)
		priorityGroup.Do(uint32(lenRule), sOutput)

		if ok, optimal := priorityGroup.IsHit(); ok {
			writer.Write(optimal)
			res = append(res, *optimal)
			cancel(sOutput.Error)
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			fmt.Println("look priorityGroup item enter!", item)
		}
		//drain(pipe)
	}, WithWorkers(len(fns)))
	return
}

func (w *WGroup) DoParallelGroup() (res []RuleOutput) {
	fns := []interface{}{}
	lenRule := len(w.Rules)
	for ruleIndex := 0; ruleIndex < lenRule; ruleIndex++ {
		fns = append(fns, w.Rules[ruleIndex])
	}
	MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		sOutput := DoRule(item)
		//fit bill. write result then cancel
		if sOutput.Status == RuleErrorStatus {
			writer.Write(sOutput)
			res = append(res, *sOutput)
			cancel(sOutput.Error)
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			fmt.Println("look DoParallelGroup item enter！", item)
		}
		//drain(pipe)
	}, WithWorkers(len(fns)))
	return
}

//control group manager
func WorkFlowManager(ruleFlow []interface{}) (res []RuleOutput) {
	for ruleGroupIndex := 0; ruleGroupIndex < len(ruleFlow); ruleGroupIndex++ {
		rule := ruleFlow[ruleGroupIndex]
		item := []RuleOutput{}
		if wGroup, ok := rule.(WGroup); ok {
			//check wGroup.type do
			switch wGroup.Type {
			case PriorityParallelType:
				item = wGroup.DoPriorityGroup();
			case ParallelType:
				item = wGroup.DoParallelGroup()
			case SerialType:
				item = wGroup.DoSerialGroup()
			default:
				WorkFlowGroup(wGroup.Rules)
			}
			if len(item) > 0 {
				return item
			}
		}
	}
	return
}

func DoRule(rule interface{}) (*RuleOutput) {
	res := reflect.ValueOf(rule).
		MethodByName("Output").
		Call([]reflect.Value{
		reflect.ValueOf(2)},
	)
	sOutput := (res[0]).Interface().(*RuleOutput)
	return sOutput
}

// DoSerialGroup
func DoSerialGroup(ruleFlow []interface{}) () {
	for ruleIndex := 0; ruleIndex < len(ruleFlow); ruleIndex++ {
		rule := ruleFlow[ruleIndex]
		state := reflect.ValueOf(rule).FieldByName("State").Int()
		//Serial do
		if state == Serial {
			sOutput := DoRule(rule)
			if sOutput.Status == RuleErrorStatus {
				fmt.Println(sOutput)
				return
			}
		} else {
			//error
		}
	}
}

// DoPriorityGroup
func DoPriorityGroup(ruleFlow []interface{}) {
	for ruleIndex := 0; ruleIndex < len(ruleFlow); ruleIndex++ {
		rule := ruleFlow[ruleIndex]
		state := reflect.ValueOf(rule).FieldByName("State").Int()
		//Parallel loading
		if state == Parallel {
			fns := []interface{}{}
			for parallelIndex := ruleIndex; parallelIndex < len(ruleFlow); parallelIndex++ {
				state := reflect.ValueOf(ruleFlow[parallelIndex]).FieldByName("State").Int()
				if state == Parallel {
					fns = append(fns, ruleFlow[parallelIndex])
				}
				//Parallel do
				if state == Serial || parallelIndex == len(ruleFlow)-1 {
					var sumPriority uint32
					var iterator uint32
					var judge uint32
					sMaxReturn := &RuleOutput{}
					MapReduceVoid(func(source chan<- interface{}) {
						for _, fn := range fns {
							source <- fn
						}
					}, func(item interface{}, writer Writer, cancel func(error)) {
						sOutput := DoRule(item)
						//.... max priority
						atomic.AddUint32(&sumPriority, sOutput.Priority)
						// 2^(n-1) + 2^(n-2) ... n=len()
						atomic.AddUint32(&judge, 2<<(uint32(len(ruleFlow))-atomic.AddUint32(&iterator, 1)))

						//fmt.Println("total", sumPriority, " iterator", iterator, " judge ", judge)
						if sOutput.Status == RuleErrorStatus && sMaxReturn.Priority < sOutput.Priority {
							sMaxReturn = sOutput
						}

						//fit bill. write result then cancel
						if sumPriority >= judge && sMaxReturn.Status == RuleErrorStatus {
							writer.Write(sMaxReturn)
							cancel(sOutput.Error)
						}
					}, func(pipe <-chan interface{}, cancel func(error)) {
						for item := range pipe {
							fmt.Println("look priorityGroup item enter!", item)
						}
						//drain(pipe)
					}, WithWorkers(len(fns)))
					//bound check
					if parallelIndex == len(ruleFlow)-1 && state == Parallel {
						return
					}
					ruleIndex = parallelIndex - 1
					break
				}
			}
		}
	}
}

// 组概念 抽象 group interface{}
// 并行优先级组的东西、、是否拿到所有并行场景的result结果
// WorkFlow
// 解析组
func WorkFlowGroup(ruleFlow []interface{}) {
	for ruleIndex := 0; ruleIndex < len(ruleFlow); ruleIndex++ {
		rule := ruleFlow[ruleIndex]
		state := reflect.ValueOf(rule).FieldByName("State").Int()
		//Serial do
		if state == Serial {
			fmt.Println(" Serial had call")
			res := reflect.ValueOf(rule).MethodByName("Output").
				Call([]reflect.Value{reflect.ValueOf(2)})
			sOutput := (res[0]).Interface().(*RuleOutput)
			//break
			if sOutput.Status == RuleErrorStatus {
				fmt.Println(sOutput)
				return
			}
		}
		//Parallel loading
		if state == Parallel {
			fns := []interface{}{}
			for parallelIndex := ruleIndex; parallelIndex < len(ruleFlow); parallelIndex++ {
				state := reflect.ValueOf(ruleFlow[parallelIndex]).FieldByName("State").Int()
				if state == Parallel {
					fns = append(fns, ruleFlow[parallelIndex])
				}
				//Parallel do
				if state == Serial || parallelIndex == len(ruleFlow)-1 {
					MapReduceVoid(func(source chan<- interface{}) {
						for _, fn := range fns {
							source <- fn
						}
					}, func(item interface{}, writer Writer, cancel func(error)) {
						fn := reflect.ValueOf(item).MethodByName("Output")
						//res [<*mr.RuleOutput Value>]
						res := fn.Call([]reflect.Value{reflect.ValueOf(2)})
						sOutput := (res[0]).Interface().(*RuleOutput)
						if sOutput.Status == RuleErrorStatus { //
							//cancel(sOutput.Error)
							//iterator...
						}
					}, func(pipe <-chan interface{}, cancel func(error)) {
						drain(pipe)
					}, WithWorkers(len(fns)))
					//bound check
					if parallelIndex == len(ruleFlow)-1 && state == Parallel {
						return
					}
					ruleIndex = parallelIndex - 1
					break
				}
			}
		}
	}
}

//解析器。。来决策是串行还是并行。
func Run() {

}
func Finish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		drain(pipe)
	}, WithWorkers(len(fns)))
}

func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	MapVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	}, WithWorkers(len(fns)))
}

func Map(generate GenerateFunc, mapper MapFunc, opts ...Option) chan interface{} {
	options := buildOptions(opts...)
	source := buildSource(generate)
	collector := make(chan interface{}, options.workers)
	done := syncx.NewDoneChan()

	go mapDispatcher(mapper, source, collector, done.Done(), options.workers)

	return collector
}

func MapReduce(generate GenerateFunc, mapper MapperFunc, reducer ReducerFunc, opts ...Option) (interface{}, error) {
	source := buildSource(generate)
	return MapReduceWithSource(source, mapper, reducer, opts...)
}

func MapReduceWithSource(source <-chan interface{}, mapper MapperFunc, reducer ReducerFunc,
	opts ...Option) (interface{}, error) {
	options := buildOptions(opts...)
	output := make(chan interface{})
	collector := make(chan interface{}, options.workers)
	done := syncx.NewDoneChan()
	writer := newGuardedWriter(output, done.Done())
	var retErr errorx.AtomicError
	cancel := once(func(err error) {
		if err != nil {
			retErr.Set(err)
		} else {
			retErr.Set(ErrCancelWithNil)
		}

		drain(source)
		done.Close()
		close(output)
	})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				cancel(fmt.Errorf("%v", r))
			}
		}()
		reducer(collector, writer, cancel)
	}()
	go mapperDispatcher(mapper, source, collector, done.Done(), cancel, options.workers)

	value, ok := <-output
	if err := retErr.Load(); err != nil {
		return nil, err
	} else if ok {
		return value, nil
	} else {
		return nil, nil
	}
}

func MapReduceVoid(generator GenerateFunc, mapper MapperFunc, reducer VoidReducerFunc, opts ...Option) error {
	_, err := MapReduce(generator, mapper, func(input <-chan interface{}, writer Writer, cancel func(error)) {
		reducer(input, cancel)
		// We need to write a placeholder to let MapReduce to continue on reducer done,
		// otherwise, all goroutines are waiting. The placeholder will be discarded by MapReduce.
		writer.Write(lang.Placeholder)
	}, opts...)
	return err
}

func MapVoid(generate GenerateFunc, mapper VoidMapFunc, opts ...Option) {
	drain(Map(generate, func(item interface{}, writer Writer) {
		mapper(item)
	}, opts...))
}

func WithWorkers(workers int) Option {
	return func(opts *mapReduceOptions) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

func buildOptions(opts ...Option) *mapReduceOptions {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func buildSource(generate GenerateFunc) chan interface{} {
	source := make(chan interface{})
	threading.GoSafe(func() {
		defer close(source)
		generate(source)
	})

	return source
}

// drain drains the channel.
func drain(channel <-chan interface{}) {
	// drain the channel
	for range channel {
	}
}

func executeMappers(mapper MapFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan lang.PlaceholderType, workers int) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(collector)
	}()

	pool := make(chan lang.PlaceholderType, workers)
	writer := newGuardedWriter(collector, done)
	for {
		select {
		case <-done:
			return
		case pool <- lang.Placeholder:
			item, ok := <-input
			if !ok {
				<-pool
				return
			}

			wg.Add(1)
			// better to safely run caller defined method
			threading.GoSafe(func() {
				defer func() {
					wg.Done()
					<-pool
				}()

				mapper(item, writer)
			})
		}
	}
}

func mapDispatcher(mapper MapFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan lang.PlaceholderType, workers int) {
	executeMappers(func(item interface{}, writer Writer) {
		mapper(item, writer)
	}, input, collector, done, workers)
}

func mapperDispatcher(mapper MapperFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan lang.PlaceholderType, cancel func(error), workers int) {
	executeMappers(func(item interface{}, writer Writer) {
		mapper(item, writer, cancel)
	}, input, collector, done, workers)
}

func newOptions() *mapReduceOptions {
	return &mapReduceOptions{
		workers: defaultWorkers,
	}
}

func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

type guardedWriter struct {
	channel chan<- interface{}
	done    <-chan lang.PlaceholderType
}

func newGuardedWriter(channel chan<- interface{}, done <-chan lang.PlaceholderType) guardedWriter {
	return guardedWriter{
		channel: channel,
		done:    done,
	}
}

func (gw guardedWriter) Write(v interface{}) {
	select {
	case <-gw.done:
		return
	default:
		gw.channel <- v
	}
}
