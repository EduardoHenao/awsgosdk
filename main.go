package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	bucketName := "hello-world-789"
	s3svc := InitAws()

	BucketsList(s3svc)

	BucketCreate(s3svc, bucketName)
	BucketsList(s3svc)

	BucketDelete(s3svc, bucketName)
	BucketsList(s3svc)
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
