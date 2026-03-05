package main

import (
	"fmt"
	"strconv"
	"time"
)

type RedisList struct {
	waiters []chan struct{}
	list    []string
}

func (c *Command) Push(right bool) Response {
	var rList *RedisList

	if len(c.Array) < 3 {
		return c.GenErrResponse("invalid arg num for lpush")
	}
	memMu.Lock()
	obj, exists := mem[string(c.Array[1].Bulk)]
	if !exists {
		rList = &RedisList{}
	} else {
		if obj.Type != TypeList {
			fmt.Println("here error 1")
			return c.GenErrResponse("type not match")
		}

		if obj.Value == nil {
			rList = &RedisList{}
		} else {
			var ok bool
			rList, ok = obj.Value.(*RedisList)
			if !ok {
				return c.GenErrResponse("inner error: type not match")
			}
		}
	}

	list := rList.list
	for i := 2; i < len(c.Array); i++ {
		if !IsBulkStr(c.Array[i].Type) {
			memMu.Unlock()
			return c.GenErrResponse("arg type invalid")
		}
		if right {
			list = append(list, string(c.Array[i].Bulk))
		} else {
			list = append([]string{string(c.Array[i].Bulk)}, list...)
		}
	}
	rList.list = list
	obj.Type = TypeList
	obj.Value = rList
	mem[string(c.Array[1].Bulk)] = obj
	length := len(list)
	wakeNum := min(len(rList.waiters), length)
	for wakeNum > 0 {
		rList.waiters[0] <- struct{}{}
		rList.waiters = rList.waiters[1:]
		wakeNum--
	}
	memMu.Unlock()
	return c.GenerateNumResponse(int64(length))
}

func (c *Command) Lpop(block bool) Response {
	popNum := 1
	if len(c.Array) != 2 && len(c.Array) != 3 {
		return c.GenErrResponse("invalid arg num for lpop")
	}

	if len(c.Array) == 3 {
		num, err := strconv.Atoi(string(c.Array[2].Bulk))
		if err != nil || num <= 0 {
			return c.GenErrResponse("lpop just support num > 0")
		}
		popNum = num
	}
	memMu.Lock()
	obj, exists := mem[string(c.Array[1].Bulk)]
	if !exists {
		return Response{Type: '$', Bulk: nil}
	}

	if obj.Type != TypeList {
		return c.GenErrResponse("type not match")
	}

	if obj.Value == nil {
		return Response{Type: '$', Bulk: nil}
	}
	rList, ok := obj.Value.(*RedisList)
	if !ok {
		return c.GenErrResponse("inner error: type not match")
	}

	if rList == nil || len(rList.list) == 0 {
		memMu.Unlock()
		return Response{Type: '$', Bulk: nil}
	}

	list := rList.list
	if popNum > len(list) {
		popNum = len(list)
	}
	rList.list = list[popNum:]
	obj.Type = TypeList
	obj.Value = rList
	mem[string(c.Array[1].Bulk)] = obj
	memMu.Unlock()

	if popNum == 1 {
		return Response{Type: '$', Bulk: []byte(list[0])}
	}
	res := Response{}
	res.Type = '*'
	res.Array = RedisArray{}

	for i := range popNum {
		tmp := RedisValue{}
		tmp.Type = '$'
		tmp.Bulk = []byte(list[i])
		res.Array = append(res.Array, tmp)
	}
	return res
}

func (c *Command) Blpop() Response {
	var waitTime float64
	var rList *RedisList
	myNotify := make(chan struct{}, 1)
	var timeoutCh <-chan time.Time

	var err error
	if len(c.Array) != 3 {
		return c.GenErrResponse("invalid arg num for lpop")
	}

	if len(c.Array) == 3 {
		strconv.ParseFloat(string(c.Array[2].Bulk), 64)
		waitTime, err = strconv.ParseFloat(string(c.Array[2].Bulk), 64)
		if err != nil || waitTime < 0 {
			return c.GenErrResponse("lpop block just support time >= 0")
		}
	}
	if waitTime > 0 {
		timeoutCh = time.After(time.Duration(waitTime * float64(time.Second)))
	}
	memMu.Lock()
	obj, exists := mem[string(c.Array[1].Bulk)]
	if !exists {
		rList = &RedisList{}
		obj.Type = TypeList
		obj.Value = rList
		mem[string(c.Array[1].Bulk)] = obj
	} else {
		var ok bool
		if obj.Type != TypeList {
			return c.GenErrResponse("type not match")
		}
		rList, ok = obj.Value.(*RedisList)
		if !ok {
			return c.GenErrResponse("inner error: type not match")
		}
	}
	for len(rList.list) == 0 {
		rList.waiters = append(rList.waiters, myNotify)
		memMu.Unlock()
		select {
		case <-myNotify:
			memMu.Lock()
		case <-timeoutCh:
			memMu.Lock()
			if len(myNotify) > 0 {
				<-myNotify
			} else {
				for i, waiter := range rList.waiters {
					if waiter == myNotify {
						rList.waiters = append(rList.waiters[:i], rList.waiters[i+1:]...)
						break
					}
				}
			}
			if len(rList.list) == 0 {
				memMu.Unlock()
				return Response{Type: '*', Array: nil}
			}
		}
	}

	list := rList.list
	rList.list = list[1:]
	obj.Type = TypeList
	obj.Value = rList
	mem[string(c.Array[1].Bulk)] = obj
	memMu.Unlock()

	res := Response{}
	res.Type = '*'
	res.Array = RedisArray{}

	tmp := RedisValue{}
	tmp.Type = '$'
	tmp.Bulk = []byte(c.Array[1].Bulk)
	res.Array = append(res.Array, tmp)

	tmp.Type = '$'
	tmp.Bulk = []byte(list[0])
	res.Array = append(res.Array, tmp)
	return res
}

func (c *Command) Llen() Response {
	if len(c.Array) != 2 {
		return c.GenErrResponse("invalid arg num for llen")
	}
	length := 0
	memMu.RLock()
	obj, exists := mem[string(c.Array[1].Bulk)]

	if exists && obj.Value != nil {
		if obj.Type != TypeList {
			memMu.RUnlock()
			return c.GenErrResponse("type not match")
		}
		rList, ok := obj.Value.(*RedisList)
		if !ok {
			memMu.RUnlock()
			return c.GenErrResponse("inner error: type not match")
		}
		length = len(rList.list)
	}
	memMu.RUnlock()
	return c.GenerateNumResponse(int64(length))
}
