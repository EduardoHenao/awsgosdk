package main

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

func main() {
	bucketName := "hello-world-789"
	s3svc := InitAws()

	BucketsList(s3svc)

	BucketCreate(s3svc, bucketName)
	BucketsList(s3svc)

	BucketObjectsCreate(s3svc, bucketName)

	objects := BucketObjectsList(s3svc, bucketName)

	BucketObjectsDelete(s3svc, bucketName, objects)

	BucketDelete(s3svc, bucketName)
	BucketsList(s3svc)
}

func BucketObjectsDelete(s3Client *s3.Client, bucketName string, keys []string) error {
	// Prepare the list of objects to delete
	var objectsToDelete []types.ObjectIdentifier
	for _, key := range keys {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: aws.String(key)})
	}

	// Call DeleteObjects to delete the specified objects
	output, err := s3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(true), // If set to true, no output is returned for each deleted object
		},
	})
	if err != nil {
		return fmt.Errorf("unable to delete objects: %v", err)
	}

	// Handle any errors from the deletion operation
	if len(output.Errors) > 0 {
		for _, errObj := range output.Errors {
			fmt.Printf("Failed to delete %s: %v\n", *errObj.Key, *errObj.Message)
		}
		return fmt.Errorf("some objects could not be deleted")
	}

	return nil
}

func BucketObjectsCreate(s3svc *s3.Client, bucketName string) {
	for i := 0; i < 5; i++ {
		fileName, err := uploadRandomFile(s3svc, bucketName, i)
		if err != nil {
			log.Fatalf("error uploading file [%v] to s3 bucket [%v]", bucketName, fileName)
		}
	}
}

func uploadRandomFile(s3Client *s3.Client, bucketName string, key int) (string, error) {
	// Generate a random file name and content
	fileName := fmt.Sprintf("file_%d.txt", key)
	fileContent := uuid.New().String()

	// Create an input for the PutObject operation
	input := &s3.PutObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(fileName),
		Body:              bytes.NewReader([]byte(fileContent)),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}

	// Upload the file
	_, err := s3Client.PutObject(context.TODO(), input)
	if err != nil {
		return "", fmt.Errorf("unable to upload file %s: %v", fileName, err)
	}

	return fileName, nil
}

func BucketObjectsList(s3svc *s3.Client, bucketName string) []string {
	var answer []string
	resp, err := s3svc.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("failed to get objects in bucket %v", err)
	}

	for _, object := range resp.Contents {
		answer = append(answer, aws.ToString(object.Key))
		log.Default().Printf("object: [%v]", object.Key)
	}
	return answer
}

func BucketDelete(s3svc *s3.Client, bucketName string) {
	_, err := s3svc.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("failed to delete bucket %v", err)
	}
}

func BucketCreate(s3svc *s3.Client, bucketName string) {
	_, err := s3svc.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("failed to create bucket %v", err)
	}
}

func BucketsList(s3svc *s3.Client) {
	resp, err := s3svc.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("failed to list buckets, %v", err)
	}

	if len(resp.Buckets) != 0 {
		fmt.Println("Buckets:")
		for _, bucket := range resp.Buckets {
			fmt.Println(aws.ToString(bucket.Name))
		}
	} else {
		fmt.Println("no buckets found")
	}
}

func InitAws() *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return s3.NewFromConfig(cfg)
}
