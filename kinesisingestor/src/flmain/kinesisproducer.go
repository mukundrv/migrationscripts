package main

import (
	//	"encoding/json"

	"bytes"
	"compress/gzip"
	"expvar"
	"flowlogs"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	//	pphttp "net/http/pprof"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	_ "sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var queue = make(chan string)

var totalSize int64

//var kpProfile *pprof.Profile

//var elapsed = expvar.NewFloat("parseTotalTime") // nanoseconds

func init() {

	//kpProfile = pprof.NewProfile("kp")
	go http.ListenAndServe(":8089", http.DefaultServeMux)
	//http.DefaultServeMux.Handle("/debug/pprof/kp", pprofHTTP.Handler("kp"))

	_ = expvar.NewFloat("parseTotalTime")

	//go statsdSender()
	//go graphiteSender()
}

func StatCount(metric string, value int) {
	//fmt.Println("statcount")
	queue <- fmt.Sprintf("%s:%d|c", metric, value)
}

func StatTime(metric string, took time.Duration) {
	//fmt.Println("stattime")
	queue <- fmt.Sprintf("%s:%d|ms", metric, took/1e6)
}

func StatGauge(metric string, value int) {
	//fmt.Println("statgauge")
	queue <- fmt.Sprintf("%s:%d|g", metric, value)
}

func GraphiteCount(metric string, value int) {
	//fmt.Println("statcount")
	x := metric + " " + strconv.Itoa(value) + " " + strconv.FormatInt(time.Now().Unix(), 10)
	_ = x
	fmt.Println(x)
	queue <- fmt.Sprintf("%s %d %d\n", metric, value, time.Now().Unix())
}

func GraphiteTime(metric string, took time.Duration) {
	//fmt.Println("statcount")
	queue <- fmt.Sprintf("%s %d %d\n", metric, took/1e6, time.Now().Unix())
	//queue <- fmt.Sprintf("%s %d %d", metric, value, )
}

func statsdSender() {
	for s := range queue {
		conn, err := net.Dial("udp", "localhost:8125")
		//conn, err := net.Dial("tcp", "localhost:8125")
		if err == nil {
			//fmt.Println("success")
			io.WriteString(conn, s)
			conn.Close()

		} else {
			fmt.Println(err)
		}

	}
}

func graphiteSender() {
	for s := range queue {
		conn, err := net.Dial("tcp", "127.0.0.1:2003")
		//conn, err := net.Dial("tcp", "localhost:8125")
		if err == nil {
			//fmt.Println(s)
			io.WriteString(conn, s)
			conn.Close()
			//fmt.Println("success")

		} else {
			fmt.Println(err)
		}

	}

}

func test() {

	x := strconv.FormatInt(time.Now().UnixNano(), 10)
	fmt.Println(time.Now())

	//x := (strconv.FormatInt(r.Int63n(255), 57))
	fmt.Println(x)
}

func main() {

	//test()
	//return
	//http.ListenAndServe(":8080", http.DefaultServeMux)

	start := time.Now().UnixNano()
	fmt.Println("Start", start)
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	streamName := os.Args[1]
	N, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("err")
	}

	batchSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("err")
	}

	//fmt.Println(os.Args[4])
	iterations, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Println("err")
	}

	fmt.Println("streamname", streamName, "N", N, "batchSize ", batchSize, "Iterations ", iterations)

	describeStream(sess, streamName)

	wg := new(sync.WaitGroup)

	for i := 0; i < N; i += 1 {
		wg.Add(1)
		//fl := flowlogs.GenerateVPCLogData(batchSize)
		//fmt.Println("X", reflect.TypeOf(fl))

		q := strconv.Itoa(i % 10)
		//fmt.Println(i % 10)
		go ingestRecords(sess, wg, string(q), streamName, batchSize, iterations)
		//go ingest(sess, wg, string(i%10), streamName, fl)
	}

	wg.Wait()

	end := time.Now().UnixNano()
	fmt.Println("end", end)
	fmt.Println("Diff", (end - start))

	//fmt.Println(len(*fl))

	//go run flmain/kinesisproducer.go vpc 20 100
}

func describeStream(sess *session.Session, streamName string) {

	svc := kinesis.New(sess)

	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName), // Required
		//ExclusiveStartShardId: aws.String("ShardId"),
		Limit: aws.Int64(1),
	}

	resp, err := svc.DescribeStream(params)
	(reflect.TypeOf(resp))

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	//	fmt.Println(*resp.StreamDescription.StreamName)
	//	fmt.Println(*resp.StreamDescription.StreamARN)
	//	fmt.Println(*resp.StreamDescription.StreamCreationTimestamp)

	/*	xx := []byte(resp.String())
		err1 := json.Unmarshal(xx, &f)
		if err1 != nil {
			fmt.Println(err.Error())
			return
		}
		m := f.(map[string]interface{})
	*/
	//fmt.Println(m["StreamDescription"])

}

func putRecord(sess *session.Session, y string, p string, streamName string) {

	svc := kinesis.New(sess)

	//buf := compressRecord(y)

	params := &kinesis.PutRecordInput{
		//Data: []byte(buf.Bytes()), // Required
		Data:         []byte(y),
		PartitionKey: aws.String(p),          // Required
		StreamName:   aws.String(streamName), // Required
		//ExplicitHashKey:           aws.String("HashKey"),
		//SequenceNumberForOrdering: aws.String("SequenceNumber"),
	}
	_, err := svc.PutRecord(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

}

func compressRecord(y string) bytes.Buffer {

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	//	defer zw.Close()

	_, err := zw.Write([]byte(y))
	if err != nil {
		log.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		log.Fatal(err)
	}

	return buf

}

//func ingest(sess *session.Session, wg *sync.WaitGroup, p string, streamName string, v []flowlogs.Vpcflowlog) {
func ingestRecord(sess *session.Session, wg *sync.WaitGroup, p string, streamName string, batchSize int, iterations int) {

	defer wg.Done()

	for i := 0; i < iterations; i++ {
		//fmt.Println("ingest start for iteration ", i)

		v := flowlogs.GenerateVPCLogData(batchSize)

		for _, x := range v {
			//fmt.Println(x)
			//w1 := strconv.FormatInt(x.Windowstart, 10)
			//w2 := strconv.FormatInt(x.Windowend, 10)

			f := []string{x.Id, x.Version, x.Account, x.Eni, x.Source, x.Destination, x.Srcport,
				x.Destport, x.Protocol, x.Packets, x.Bytes, x.Windowstart, x.Windowend, x.Action, x.Status,
			}

			y := strings.Join(f, ",")
			putRecord(sess, y, p, streamName)
		}

	}
	//fmt.Println("ingest end")
}

func createPutRecordsRequestEntry(y string, p string) *kinesis.PutRecordsRequestEntry {

	f := []string{"0", "68056473384187692692674921486353642291", "136112946768375385385349842972707284582", "204169420152563078078024764459060926873", "272225893536750770770699685945414569164"}
	g, err := strconv.Atoi(p)
	h := g % len(f)
	//fmt.Println(g)
	_ = err

	i := &kinesis.PutRecordsRequestEntry{
		Data:            []byte(y),
		PartitionKey:    aws.String(p),
		ExplicitHashKey: aws.String(f[h]),
	}

	return i
}

func putRecords(sess *session.Session, v []flowlogs.Vpcflowlog, p string, streamName string) {

	svc := kinesis.New(sess)

	r := []*kinesis.PutRecordsRequestEntry{}

	for _, x := range v {
		//fmt.Println(x)
		//w1 := strconv.FormatInt(x.Windowstart, 10)
		//w2 := strconv.FormatInt(x.Windowend, 10)

		f := []string{x.Id, x.Version, x.Account, x.Eni, x.Source, x.Destination, x.Srcport,
			x.Destport, x.Protocol, x.Packets, x.Bytes, x.Windowstart, x.Windowend, x.Action, x.Status,
		}

		y := strings.Join(f, " ")
		fmt.Println(y)
		rEntry := createPutRecordsRequestEntry(y, p)

		r = append(r, rEntry)

	}

	params := &kinesis.PutRecordsInput{
		Records:    r,
		StreamName: aws.String(streamName),
	}
	resp, err := svc.PutRecords(params)

	fmt.Println(resp)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

}

func ingestRecords(sess *session.Session, wg *sync.WaitGroup, p string, streamName string, batchSize int, iterations int) {

	defer wg.Done()

	//t1 := time.Now()
	defer func() {
		//elapsed.Add(time.Since(t1).Seconds())
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}

	}()

	for i := 0; i < iterations; i++ {
		t2 := time.Now()
		v := flowlogs.GenerateVPCLogData(batchSize)
		tx := time.Since(t2)
		_ = tx
		//fmt.Println(v)

		putRecords(sess, v, p, streamName)
		//GraphiteCount("go.kp.count", len(v))
		//atomic.AddInt64(&totalSize, int64(len(v)))
		//StatCount("go.kp.batchProcessed", len(v))
		//StatTime("go.kp.iterationProcessTime", tx*1000)
		//StatGauge("go.kp.totalProcessed", int(totalSize))
		//fmt.Println(totalSize)

	}

}
