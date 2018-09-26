package main

import (
	//	"bytes"
	//"bufio"
	"compress/gzip"
	"fmt"
	//"log"
	//"bytes"
	"io"
	//"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	a "github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {

	sourceBucket := "tpchphase3"
	var sourceObjectPrefix string

	tables := []string{"part"}

	destinationBucket := "gcp-datasets"
	var destinationObjectPrefix string

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))


	for _, t := range tables {

		sourceObjectPrefix = "1T/csvplain/" + t + "/"
		destinationObjectPrefix = "tpch_1tb/csv/" + t + "_v2/"
		fmt.Println("Start program")
		var ignoreListF = list_objects_v2(destinationBucket, destinationObjectPrefix, sess, "us-east-1", false)
		var ignoreList = []string{}

		for _, f := range ignoreListF {

			outputFile := strings.TrimSuffix(filepath.Base(f), ".gz") + ".dat"
			ignoreList = append(ignoreList, outputFile)

		}


		fmt.Println(" List Objects ffrom S3")
		var objList = list_objects_v2(sourceBucket, sourceObjectPrefix, sess, "us-west-2", false)
		fmt.Println(objList)
		finalList := difference(objList, ignoreList)
		fmt.Println(finalList)

		if len(objList) < 1 {
			fmt.Println("No objects found - ")
			return
		}

		// Make the data into N slices -- Completed
		fmt.Println(" Split Objects ")
		var divided = splitObjectsIntoNSlices(finalList)


		wg := new(sync.WaitGroup)
		fmt.Println(" process files from S3")
		for _, fileSlice := range divided {
			wg.Add(1)
			go processFiles(sourceBucket, sourceObjectPrefix, destinationBucket, destinationObjectPrefix, fileSlice, sess, true, wg)

		}

		wg.Wait()

	}

}

func processFiles(sourceBucket string, sourceObjectPrefix string, destinationBucket string, destinationObjectPrefix string, fileSlice []string, sess *session.Session, compress bool, wg *sync.WaitGroup) {

	defer wg.Done()
	fmt.Println(" process files from S3****")
	for _, file := range fileSlice {

		fmt.Println(file)
		downloadedFile := filepath.Base(file)


		fmt.Println("source bucket " + sourceBucket)
		fmt.Println("source object path " + file)
		fmt.Println("destination bucket  " + destinationBucket)
		fmt.Println("destination object path  " + destinationObjectPrefix)
		fmt.Println("Downloaded file " + downloadedFile)

		downloadFile(sourceBucket, file, downloadedFile, sess, "us-west-2")
		var outputFile = ""
		if compress {
			outputFile = strings.TrimSuffix(downloadedFile, ".dat") + ".gz"
			compressFile(downloadedFile, outputFile)
		} else {
			outputFile = strings.TrimSuffix(downloadedFile, ".gz") + ".csv"
			decompressFile(downloadedFile, outputFile)
		}
		fmt.Println("Output file  = " + outputFile)
		uploadFile(destinationBucket, destinationObjectPrefix+outputFile, outputFile, sess, "us-east-1")
		deleteFile(downloadedFile)
		deleteFile(outputFile)
	}
}

func deleteFile(localFile string) {

	fmt.Println("Deleting file " + localFile)

	var err = os.Remove(localFile)
	if err != nil {
		exitErrorf("Errorr while deleting file ", err)
		os.Exit(1)
	}

}

func decompressFile(filename string, outputFile string) {
	fmt.Println("Decompresssng file " + filename)

	if filename == "" {
		fmt.Println("Usage : gunzip sourcefile.gz")
		os.Exit(1)
	}

	gzipfile, err := os.Open(filename)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		fmt.Println("Error creating reader")
		fmt.Println(err)
		os.Exit(1)
	}
	defer reader.Close()

	writer, err := os.Create(outputFile)

	if err != nil {
		fmt.Println("Error creating writer ")
		fmt.Println(err)
		os.Exit(1)
	}

	defer writer.Close()

	if _, err = io.Copy(writer, reader); err != nil {
		fmt.Println("Error cooying file")
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Decompresssng file " + filename + " complete")
}

func compressFile(filename string, outputFile string) error {

	fmt.Println("Compress starts ")
	rawfile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer rawfile.Close()

	gzipfile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer gzipfile.Close()

	writer := gzip.NewWriter(gzipfile)

	_, err = io.Copy(writer, rawfile)
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}
	fmt.Println("Compress ends ")
	return nil
}

func uploadFile(destinationBucket string, objectPath string, localFile string, sess *session.Session, region string) {

	fmt.Println("uploading file " + localFile)

	svc := s3.New(sess, &aws.Config{Region: aws.String(region)})

	// Create S3 service client
	//svc := s3.New(sess)

	file, err := os.Open(localFile)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}
	defer file.Close()

	//uploader := s3manager.NewUploader(sess)

	uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = 20 * 1024 * 1024 // 64MB per part
	})
	uploader.Concurrency = runtime.NumCPU()

	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(destinationBucket),
		Key:    aws.String(objectPath),
		Body:   file,
	}

	_, err = uploader.Upload(uploadInput)
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", localFile, destinationBucket, err)
	}

	fmt.Printf("Successfully uploaded %q to %q\n", localFile, destinationBucket)

}

func downloadFile(sourceBucket string, objectPath string, localFile string, sess *session.Session, region string) {

	fmt.Println("dowloading file " + localFile)
	fmt.Println("object Path" + objectPath)

	svc := s3.New(sess, &aws.Config{Region: aws.String(region)})

	//downloader := s3manager.NewDownloaderWithClient(svc_west)

	downloader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
		d.PartSize = 20 * 1024 * 1024 // 64MB per part
	})

	downloader.Concurrency = runtime.NumCPU()

	/*downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
	})
	*/

	start := time.Now()

	s3obj := &s3.GetObjectInput{
		Bucket: &sourceBucket,
		Key:    &objectPath,
	}

	f, err := os.Create(localFile)
	if err != nil {

		exitErrorf("Error createing local file ", err)
	}
	defer f.Close()

	size, err := downloader.Download(f, s3obj)

	elapsed := time.Since(start)
	if err != nil {
		exitErrorf("Error downloading to local file ", err)
	}

	fmt.Println("Downloaded:", localFile, float64(size/1048576)/elapsed.Seconds(), "MB/s")
}

func splitObjectsIntoNSlices(objList []string) [][]string {

	var divided [][]string

	//numCPU := runtime.NumCPU()
	//chunkSize := (len(objList) + numCPU - 1) / numCPU
	chunkSize := 50
	for i := 0; i < len(objList); i += chunkSize {
		end := i + chunkSize

		if end > len(objList) {
			end = len(objList)
		}

		divided = append(divided, objList[i:end])
	}

	return divided


}

func list_objects_v2(bucket string, prefix string, sess *session.Session, region string, checkgz bool) []string {


	c := &aws.Config{
		Region: aws.String(region),
	}
	svc := s3.New(sess, c)

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), // Required
		Prefix: aws.String(prefix),
	}
	resp, err := svc.ListObjectsV2(params)

	var s []string

	if err != nil {
		fmt.Println(err.Error())
		return s
	}

	for _, b := range resp.Contents {

		x := aws.StringValue(b.Key)

		if strings.HasSuffix(x, "/") {
			continue
		}
		if checkgz {
			if strings.HasSuffix(x, "gz") {
				s = append(s, x)
			}
		} else {
			s = append(s, x)
		}

	}

	return s

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func difference(X, Y []string) []string {
	m := make(map[string]int)

	for _, y := range Y {
		m[y]++
	}

	var ret []string
	for _, x := range X {
		f := filepath.Base(x)
		if m[f] > 0 {
			m[f]--
			continue
		}
		ret = append(ret, x)
	}

	return ret
}

func test() {
	compressFile("./test.dat", "./test.gz")

}
