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
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	s3svc := s3.NewFromConfig(cfg)

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
