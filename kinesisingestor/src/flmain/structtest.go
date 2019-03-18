package main

import (
	_ "encoding/json"
	"fmt"
	"io"
	_ "math/rand"
	"net"
	_ "reflect"
	"time"
)

func init() {

	//t := time.Now().Unix()
	//fmt.Println(t)
	s := "test1.count 10 1491377976\n"
	conn, err := net.Dial("tcp", "localhost:2003")
	//conn, err := net.Dial("tcp", "localhost:8125")
	if err == nil {
		fmt.Println("success")
		io.WriteString(conn, s)
		conn.Close()

	} else {
		fmt.Println(err)
	}

}

func main() {

	fmt.Println(time.Now().Unix())

	return

	a := []string{"1", "2", "3"}
	fmt.Println(&a)
	test(a[:])
	testInt(1)
	x := 1
	fmt.Println(&x)
}

func test(b []string) {
	fmt.Println(&b)
	b = append(b, "4")
}

func testInt(a int) {
	fmt.Println(&a)
}
