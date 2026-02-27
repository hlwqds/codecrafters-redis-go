package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_         = net.Listen
	_         = os.Exit
	mem       = map[string]RedisMemValue{}
	memMu     sync.RWMutex
	listMem   = map[string][]string{}
	listMemMu sync.RWMutex
)

type RedisMemValue struct {
	value    string
	expireAt time.Time
}

const (
	MaxBulkSize     = 512 * 1024 * 1024
	MaxArrStackSize = 1024
	NilBulkStr      = "$-1\r\n"
	NilArr          = "*-1\r\n"
)

type (
	BulkString   []byte
	SimpleString string
	RedisArray   []RedisValue
	Command      RedisValue
	Response     RedisValue
	RedisErr     string
	RedisInt     int64
)

func IsBulkStr(t byte) bool {
	return t == '$'
}

type RedisValue struct {
	// 记录是 '+' ':' '$' '*' '-'
	Type    byte
	SimpStr SimpleString
	Num     RedisInt
	Bulk    BulkString
	Array   RedisArray
	Err     RedisErr
}

func (c *Command) GenErrResponse(msg string) Response {
	return Response{Type: '-', Err: RedisErr(msg)}
}

func (c *Command) GenOkResponse() Response {
	return Response{Type: '+', SimpStr: "OK"}
}

func (c *Command) Ping() Response {
	return Response{Type: '+', SimpStr: "PONG"}
}

func (c *Command) Echo() Response {
	if len(c.Array) != 2 {
		return c.GenErrResponse("invalid arg num for echo")
	}
	return Response(c.Array[1])
}

func (c *Command) Set() Response {
	now := time.Now()
	if len(c.Array) != 3 && len(c.Array) != 5 {
		return c.GenErrResponse("invalid arg num for set")
	}

	if !IsBulkStr(c.Array[1].Type) {
		return c.GenErrResponse("invalid arg type for set")
	}
	key := c.Array[1].Bulk

	if !IsBulkStr(c.Array[2].Type) {
		return c.GenErrResponse("invalid arg type for set")
	}
	value := c.Array[2].Bulk
	if len(c.Array) == 3 {
		memMu.Lock()
		mem[string(key)] = RedisMemValue{value: string(value)}
		memMu.Unlock()
		return c.GenOkResponse()
	}

	if !IsBulkStr(c.Array[3].Type) {
		return c.GenErrResponse("invalid arg type for set")
	}
	option := c.Array[3].Bulk

	if !IsBulkStr(c.Array[4].Type) {
		return c.GenErrResponse("invalid arg type for set")
	}
	expire, _ := strconv.Atoi(string(c.Array[4].Bulk))
	switch strings.ToLower(string(option)) {
	case "ex":
		expire *= 1000
	case "px":
	}

	memMu.Lock()
	mem[string(key)] = RedisMemValue{value: string(value), expireAt: now.Add(time.Millisecond * time.Duration(expire))}
	memMu.Unlock()

	return c.GenOkResponse()
}

func (c *Command) Get() Response {
	now := time.Now()
	if len(c.Array) != 2 || !IsBulkStr(c.Array[1].Type) {
		return c.GenErrResponse("invalid arg num for get")
	}
	memMu.RLock()
	v, exists := mem[string(c.Array[1].Bulk)]
	memMu.RUnlock()
	var res Response
	res.Type = '$'
	if !exists {
		res.Bulk = nil
	} else {
		if !v.expireAt.IsZero() && v.expireAt.Before(now) {
			res.Bulk = nil
		} else {
			res.Bulk = BulkString(v.value)
		}
	}
	return res
}

func (c *Command) GenerateNumResponse(num int64) Response {
	return Response{Type: ':', Num: RedisInt(num)}
}

func (c *Command) Push(right bool) Response {
	if len(c.Array) < 3 {
		return c.GenErrResponse("invalid arg num for lpush")
	}
	listMemMu.Lock()
	list := listMem[string(c.Array[1].Bulk)]
	for i := 2; i < len(c.Array); i++ {
		if !IsBulkStr(c.Array[i].Type) {
			listMemMu.Unlock()
			return c.GenErrResponse("arg type invalid")
		}
		if right {
			list = append(list, string(c.Array[i].Bulk))
		} else {
			list = append([]string{string(c.Array[i].Bulk)}, list...)
		}
	}
	listMem[string(c.Array[1].Bulk)] = list
	length := len(list)
	listMemMu.Unlock()
	return c.GenerateNumResponse(int64(length))
}

func (c *Command) Lrange() Response {
	if len(c.Array) != 4 {
		return c.GenErrResponse("invalid arg num for lrange")
	}
	left, err := strconv.Atoi(string(c.Array[2].Bulk))
	if err != nil {
		return c.GenErrResponse("invalid left for lrange")
	}
	right, err := strconv.Atoi(string(c.Array[3].Bulk))
	if err != nil {
		return c.GenErrResponse("invalid right for lrange")
	}
	res := Response{}
	res.Type = '*'
	res.Array = RedisArray{}
	listMemMu.RLock()
	list := listMem[string(c.Array[1].Bulk)]
	length := len(list)
	if left < -length {
		left = 0
	}
	if right < -length {
		right = 0
	}
	if left < 0 {
		left += length
	}
	if right < 0 {
		right += length
	}

	if right >= length {
		right = length - 1
	}
	for left <= right {
		tmp := RedisValue{}
		tmp.Type = '$'
		tmp.Bulk = []byte(list[left])
		res.Array = append(res.Array, tmp)
		left++
	}
	listMemMu.RUnlock()
	return res
}

func (c *Command) Process() Response {
	if c.Type != '*' {
		return c.GenErrResponse("request must be arr")
	}
	if len(c.Array) == 0 {
		return Response(*c)
	}

	command := c.Array[0]
	if command.Type != '$' {
		return c.GenErrResponse("command must be bulk string")
	}

	switch strings.ToLower(string(command.Bulk)) {
	case "ping":
		return c.Ping()
	case "echo":
		return c.Echo()
	case "set":
		return c.Set()
	case "get":
		return c.Get()
	case "rpush":
		return c.Push(true)
	case "lpush":
		return c.Push(false)
	case "lrange":
		return c.Lrange()
	default:
		return c.GenErrResponse("unknown command")
	}
}

func (b BulkString) WriteTo(w *bufio.Writer) error {
	var err error
	if b == nil {
		_, err = w.WriteString(NilBulkStr)
		return err
	}
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(b)))
	w.WriteString("\r\n")
	w.Write(b)
	_, err = w.WriteString("\r\n")
	return err
}

func (b RedisInt) WriteTo(w *bufio.Writer) error {
	w.WriteByte(':')
	if b < 0 {
		w.WriteByte('-')
	}

	w.WriteString(strconv.FormatInt(int64(b), 10))
	_, err := w.WriteString("\r\n")
	return err
}

func (s SimpleString) WriteTo(w *bufio.Writer) error {
	w.WriteByte('+')
	w.WriteString(string(s))
	_, err := w.WriteString("\r\n")
	return err
}

func (e RedisErr) WriteTo(w *bufio.Writer) error {
	w.WriteByte('-')
	w.WriteString(string(e))
	_, err := w.WriteString("\r\n")
	return err
}

func (a RedisArray) WriteTo(w *bufio.Writer) error {
	var err error
	if a == nil {
		_, err = w.WriteString(NilArr)
		return err
	}
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(a)))
	_, err = w.WriteString("\r\n")
	if err != nil {
		return err
	}
	for i := range a {
		err = a[i].WriteTo(w)
		if err != nil {
			break
		}
	}
	return err
}

func (v RedisValue) WriteTo(w *bufio.Writer) error {
	switch v.Type {
	case '+':
		return v.SimpStr.WriteTo(w)
	case '-':
		return v.Err.WriteTo(w)
	case '*':
		return v.Array.WriteTo(w)
	case '$':
		return v.Bulk.WriteTo(w)
	case ':':
		return v.Num.WriteTo(w)
	default:
		return nil
	}
}

type RedisReader struct {
	reader *bufio.Reader
}

func NewRedisReader(reader io.Reader) *RedisReader {
	return &RedisReader{bufio.NewReader(reader)}
}

func (r *RedisReader) ReadSimpleString() (SimpleString, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if line[len(line)-2] != '\r' {
		return "", errors.New("invalid req")
	}
	line = line[:len(line)-2]
	return SimpleString(line), nil
}

func (r *RedisReader) ReadBulkString() (BulkString, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]
	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 || length > MaxBulkSize {
		return nil, errors.New("length invalid")
	}

	value := make([]byte, length)
	_, err = io.ReadFull(r.reader, value)
	if err != nil {
		return nil, err
	}
	b, err := r.reader.ReadByte()
	if err != nil || b != '\r' {
		return nil, err
	}
	b, err = r.reader.ReadByte()
	if err != nil || b != '\n' {
		return nil, err
	}
	return value, nil
}

func (r *RedisReader) ReadArr(stack int) (RedisArray, error) {
	stack++
	if stack > MaxArrStackSize {
		return nil, errors.New("arr stack over flow")
	}
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-2]
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if num == -1 {
		return nil, nil
	}
	if num < 0 {
		return nil, errors.New("num invalid")
	}
	arr := make(RedisArray, num)
	for i := range num {
		arr[i], err = r.ReadValue(stack)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func (r *RedisReader) ReadValue(stack int) (RedisValue, error) {
	var value RedisValue
	t, err := r.reader.ReadByte()
	if err != nil {
		return RedisValue{}, err
	}
	switch t {
	case '$':
		value.Bulk, err = r.ReadBulkString()
	case '+':
		value.SimpStr, err = r.ReadSimpleString()
	case '*':
		value.Array, err = r.ReadArr(stack)
	default:
		err = errors.New("unknown type")
	}
	if err != nil {
		return RedisValue{}, err
	}
	value.Type = t

	return value, nil
}

func (r *RedisReader) ReadCommand() (Command, error) {
	command, err := r.ReadValue(0)
	if err != nil {
		return Command{}, err
	}

	return Command(command), nil
}

func (r *Response) WriteTo(w *bufio.Writer) error {
	if err := RedisValue(*r).WriteTo(w); err != nil {
		return err
	}
	return w.Flush()
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func(conn net.Conn) {
			defer conn.Close()
			reader := NewRedisReader(conn)
			writer := bufio.NewWriter(conn)
			for {
				command, err := reader.ReadCommand()
				if err != nil {
					return
				}
				resp := command.Process()
				if resp.WriteTo(writer) != nil {
					return
				}
			}
		}(conn)
	}
}
