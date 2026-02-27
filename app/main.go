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
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_   = net.Listen
	_   = os.Exit
	mem = map[string]string{}
)

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
)

func IsBulkStr(t byte) bool {
	return t == '$'
}

type RedisValue struct {
	// 记录是 '+' ':' '$' '*' '-'
	Type    byte
	SimpStr SimpleString
	Num     int64
	Bulk    BulkString
	Array   RedisArray
	Err     RedisErr
}

func (c *Command) Process() Response {
	res := Response{}
	if c.Type != '*' {
		res.Type = '-'
		res.Err = "request must be arr"
		return res
	}
	if len(c.Array) == 0 {
		return Response(*c)
	}

	command := c.Array[0]
	if command.Type != '$' {
		res.Type = '-'
		res.Err = "command must be bulk string"
		return res
	}

	switch strings.ToLower(string(command.Bulk)) {
	case "ping":
		res.Type = '+'
		res.SimpStr = "PONG"
	case "echo":
		if len(c.Array) != 2 {
			res.Type = '-'
			res.Err = "invalid arg num for echo"
		} else {
			res = Response(c.Array[1])
		}
	case "set":
		if len(c.Array) != 3 || (!IsBulkStr(c.Array[1].Type) || !IsBulkStr(c.Array[2].Type)) {
			res.Type = '-'
			res.Err = "invalid arg num for set"
		} else {
			mem[string(c.Array[1].Bulk)] = string(c.Array[2].Bulk)
			res.Type = '+'
			res.SimpStr = "OK"
		}
	case "get":
		if len(c.Array) != 2 || !IsBulkStr(c.Array[1].Type) {
			res.Type = '-'
			res.Err = "invalid arg num for get"
		} else {
			v, exists := mem[string(c.Array[1].Bulk)]
			res.Type = '$'
			if !exists {
				res.Bulk = BulkString(NilBulkStr)
			} else {
				res.Bulk = BulkString(v)
			}
		}
	default:
		res.Type = '-'
		res.Err = "unknown command"
	}

	return res
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
