package flowlogs

import (
	//	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	//	"strings"
	"time"
)

type Vpcflowlog struct {
	Id          string
	Version     string
	Account     string
	Eni         string
	Source      string
	Destination string
	Srcport     string
	Destport    string
	Protocol    string
	Packets     string
	Bytes       string
	Windowstart string
	Windowend   string
	Action      string
	Status      string
}

//svc := s3.New(sess, &aws.Config{Region: aws.String(region)})
var r = rand.New(rand.NewSource(time.Now().UnixNano()))

var account []string
var eni []string
var source []string
var destination []string
var srcport []string
var destport []string
var protocol []string
var packets []string
var bytes []string
var windowstart []int64
var windowend []int64
var action []string
var status []string

var ParseFailures uint64

/*func (v Vpcflowlog) String() string {

	s := []string{strconv.ParseInt(v.Version, 10, 64), v.Account, v.Eni, v.Source, v.Destination, strconv.Atoi(v.Srcport), strconv.Atoi(v.Destport), strconv.Atoi(v.Protocol), strconv.Atoi(v.Packets), strconv.Atoi(v.Bytes), strconv.Atoi(v.Windowstart), strconv.Atoi(v.Windowend), v.Action, v.Status}
	return strings.Join(s, " ")
}
*/

func Test() {
	//fmt.Println("test")
	//fmt.Println(account)
	//fmt.Println(&account)
	//fmt.Println(account)

	//fmt.Println(pickAccount())
}

func main() {

	//Test()
	//return

	fmt.Println("Main  Start ")

	x := GenerateVPCLogData(1)
	fmt.Println(x)
	//pushDatatoKinesis()

	fmt.Println("Main End")
}

func pushDatatoKinesis() {
	fmt.Println("Kinesis Push Start")

	fmt.Println("Kinesis Push End")
}

func init() {
	//fmt.Println("init")

	account = generateAccount(10)
	eni = generateENI(100)
	source = generateIPNew(1000)
	destination = generateIPNew(1000)
	srcport = generateString(1000, 0, 65536)
	destport = generateString(1000, 0, 65536)
	protocol = []string{"6", "17"}
	action = []string{"ACCEPT", "REJECT"}
	//flowlogstatus = []string{"OK"}

	//fmt.Println("Start", time.Now().UnixNano())

	//fmt.Println("Stop", time.Now().UnixNano())

}

//func GenerateVPCLogData(N int) *[]Vpcflowlog {
func GenerateVPCLogData(N int) []Vpcflowlog {
	//fmt.Println("Generate VPC LogData - Start")

	t := time.Now()

	defer func() {
		elapsed := time.Since(t)
		_ = elapsed
		//fmt.Println("Elapsed time ", elapsed)
	}()
	//start := time.Now().UnixNano()
	//fmt.Println("Start", start)
	//var v []Vpcflowlog
	//v := make([]Vpcflowlog, N)
	v := make([]Vpcflowlog, N)

	for i := 0; i < N; i += 1 {
		vx := Vpcflowlog{}

		vx.Id = getID()
		vx.Version = "2"
		vx.Account = pickAccount()
		vx.Eni = pickENI()
		//fmt.Println(vx.Eni)
		vx.Source = pickString(source)
		vx.Destination = pickString(destination)
		vx.Srcport = pickString(srcport)
		vx.Destport = pickString(destport)
		vx.Protocol = pickString(protocol)

		p := getRandomNumber(1, 10000)
		vx.Packets = strconv.Itoa(p)
		vx.Bytes = strconv.Itoa(p * getRandomNumber(1, 1000))
		startTime := getRandomNumber(1451606400, 1483228800)
		vx.Windowstart = strconv.Itoa(startTime)
		vx.Windowend = strconv.Itoa(startTime + getRandomNumber(50, 500))
		vx.Action = pickString(action)
		vx.Status = "OK"

		v[i] = vx
		//v = append(v, vx)
		//fmt.Println(vx)
	}
	//end := time.Now().UnixNano()
	//fmt.Println("end", end)
	//fmt.Println("Diff", (end - start))
	//fmt.Println("Generate VPC LogData - End")

	return v[:]

}

var f interface{}

func getID() string {

	//x := (strconv.FormatInt(r.Int63n(255), 57))
	x := strconv.FormatInt(time.Now().UnixNano(), 10)
	return x
}

func generateAccount(N int) []string {

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const nums = "0123456789"
	res := make([]string, N)

	for i := 0; i < N; i += 1 {
		r2 := make([]byte, 12)
		for j := range r2 {
			//r2[j] = nums[r.Int63()%int64(len(nums))]
			r2[j] = nums[rand.Intn(len(nums))]
		}
		res[i] = string(r2)
		//fmt.Println(string(r2))

	}

	//fmt.Println(res)

	return res
}

func generateENI(N int) []string {

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const chars = "abcdefghijklmnopqrstuvwxyz"
	const nums = "0123456789"

	res := make([]string, N)

	r0 := []byte("eni-")

	for i := 0; i < N; i += 1 {

		r1 := make([]byte, 3)
		for i := range r1 {
			r1[i] = chars[rand.Intn(len(chars))]
		}

		r2 := make([]byte, 2)
		for j := range r2 {
			r2[j] = nums[rand.Intn(len(nums))]
		}

		r3 := make([]byte, 2)
		for k := range r3 {
			r3[k] = chars[rand.Intn(len(nums))]
		}

		//res[i] := stringCopyNew(string(r1), string(r2), string(r3))
		res[i] = stringCopyNew(r0, r1, r2, r3)
		//fmt.Println(string(res[i]))

	}
	return res

}

/*
func generateIP(N int) []string {

	res := make([]string, N)
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < N; i += 1 {
		x0 := (strconv.FormatInt(r.Int63n(255), 10))
		x1 := (strconv.FormatInt(r.Int63n(255), 10))
		x2 := (strconv.FormatInt(r.Int63n(255), 10))
		x3 := (strconv.FormatInt(r.Int63n(255), 10))
		//fmt.Println(x0)
		//fmt.Println(x1)
		//fmt.Println(x2)
		//fmt.Println(x3)

		ip := stringCopyNew([]byte(x0), []byte("."), []byte(x1), []byte("."), []byte(x2), []byte("."), []byte(x3))
		res[i] = string(ip)
		//fmt.Println((res[i]))
	}
	return res
}
*/

func generateIPNew(N int) []string {

	res := make([]string, N)
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < N; i += 1 {
		x0 := strconv.Itoa(rand.Intn(255-1) + 1)
		x1 := strconv.Itoa(rand.Intn(255))
		x2 := strconv.Itoa(rand.Intn(255))
		x3 := strconv.Itoa(rand.Intn(255))
		//fmt.Println(x0)
		//fmt.Println(x1)
		//fmt.Println(x2)
		//fmt.Println(x3)

		ip := stringCopyNew([]byte(x0), []byte("."), []byte(x1), []byte("."), []byte(x2), []byte("."), []byte(x3))
		res[i] = string(ip)
		//fmt.Println((res[i]))
	}
	return res
}

func generateNumber(N int, min int, max int) []int {

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	res := make([]int, N)
	for i := 0; i < N; i += 1 {
		res[i] = rand.Intn(max-min) + min
		//fmt.Println(res[i])

	}
	return res
}

func generateString(N int, min int, max int) []string {

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	res := make([]string, N)
	for i := 0; i < N; i += 1 {
		res[i] = strconv.Itoa(r.Intn(max-min) + min)
		//fmt.Println(res[i])

	}
	return res
}

func generateTimestamp() time.Time {
	randomTime := rand.Int63n(time.Now().Unix()-94608000) + 94608000

	randomNow := time.Unix(randomTime, 0)

	return randomNow
}

/*
func stringCopy(str1 string, str2 string) string {

	b := make([]byte, len(str1)+len(str2))
	bp := copy(b, str1)
	bp += copy(b[bp:], str2)

	return string(b)
}
*/

func stringCopyNew(str ...[]byte) string {

	var totalLen = 0
	for i := 0; i < len(str); i += 1 {
		totalLen += len(str[i])
	}

	b := make([]byte, totalLen)
	bq := 0
	for i := 0; i < len(str); i += 1 {
		bq += copy(b[bq:], str[i])

	}
	return string(b)
}

func printLenOfArray(array []string) {

	for i := 0; i < len(array); i += 1 {

		fmt.Println("index ", i, array[i], len(array[i]))
	}
}

func pickVersion() int {

	return 2
}

func pickAccount() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in pickAccount", r)
		}
	}()

	//index := r.Int63() % int64(len(account)-1)
	//var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	index := rand.Intn(len(account))
	//fmt.Println(index)
	return account[index]
}

func pickENI() string {

	//eni := generateENI(200)

	//index := rand.Int63() % int64(len(eni))

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in picENI", r)
		}
	}()

	index := rand.Intn(len(eni))
	return eni[index]
}

func pickString(source []string) string {
	//index := r.Int63() % int64(len(source)-1)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in pickString", r)
		}
	}()

	index := rand.Intn(len(source))
	return source[index]
}

func pickNumber(source []int) int {
	//index := r.Int63() % int64(len(source)-1)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in pickNumber", r)
		}
	}()
	index := rand.Intn(len(source))
	return source[index]
}

func getRandomNumber(min int, max int) int {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in getRandomNumber", r)
		}
	}()
	x := rand.Intn(max - min)
	//fmt.Println("getRandomNumber", x)
	return x + min
}
