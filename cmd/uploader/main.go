package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Client *s3.S3
	s3Bucket string
	wg       sync.WaitGroup
)

func init() {
	//initianing AWS session
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("us-east-1"),
			Credentials: credentials.NewStaticCredentials(
				"",
				"",
				"",
			),
		},
	)
	if err != nil {
		panic(err)
	}
	s3Client = s3.New(sess)
	s3Bucket = "rps-bucket-test"
}
func main() {

	// defining directory source
	dir, err := os.Open("./tmp")
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	//creating upload control channel to make sure only 100 concurrent uploads happen at the same time
	uploadControl := make(chan struct{}, 100)
	//creating error channel to make sure only 10 concurrent uploads happen at the same time
	errorFileUpload := make(chan string, 10)

	// new anon func to check if a file was not uploaded then try to upload it again
	go func() {
		for {
			select {
			case fileName := <-errorFileUpload:
				wg.Add(1)
				uploadControl <- struct{}{}
				go uploadFile(fileName, uploadControl, errorFileUpload)
			}
		}
	}()

	//reading all files from directory
	for {
		//reading only the first file found
		files, err := dir.Readdir(1)

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading directory: %s\n", err)
			continue
		}
		//wg cotrol to garantee program execution
		wg.Add(1)
		//informing a new file is being uploaded
		uploadControl <- struct{}{}
		//uploading file
		go uploadFile(files[0].Name(), uploadControl, errorFileUpload)
	}
	wg.Wait()
}

func uploadFile(filename string, uc <-chan struct{}, efu chan<- string) {
	wg.Done()
	completeFileName := fmt.Sprintf("./tmp/%s", filename)
	fmt.Printf("Uploading file: %s to bucket %s\n ", completeFileName, s3Bucket)
	f, err := os.Open(completeFileName)
	if err != nil {
		fmt.Printf("Error opening file %s \n", completeFileName)
		<-uc //empty upload controller
		efu <- completeFileName
		return
	}
	defer f.Close()
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(filename),
		Body:   f,
	})
	if err != nil {
		fmt.Printf("Error uploading file %s \n", completeFileName)
		<-uc //empty upload controller
		efu <- completeFileName
		return
	}
	fmt.Printf("Successfully uploaded file %s \n", completeFileName)
	<-uc //empty upload controller
}
