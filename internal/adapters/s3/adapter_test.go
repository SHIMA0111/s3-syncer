package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockS3Client struct {
	CopyObjectFn    func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2Fn func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObjectFn     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObjectFn    func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

func (m *mockS3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return m.CopyObjectFn(ctx, params, optFns...)
}
func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.ListObjectsV2Fn(ctx, params, optFns...)
}
func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.GetObjectFn(ctx, params, optFns...)
}
func (m *mockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m.HeadObjectFn(ctx, params, optFns...)
}

type mockUploader struct {
	UploadFn func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func (m *mockUploader) Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	return m.UploadFn(ctx, input, opts...)
}

func TestAdapter_GetAccountID(t *testing.T) {
	a := &Adapter{accountID: "1234"}
	id, _ := a.GetAccountID(context.Background())
	if id != "1234" {
		t.Errorf("Expected 1234, got %s", id)
	}
}

func TestAdapter_GetBucketName(t *testing.T) {
	a := &Adapter{bucket: "my-bucket"}
	if a.GetBucketName() != "my-bucket" {
		t.Errorf("Expected my-bucket, got %s", a.GetBucketName())
	}
}

func TestAdapter_CopyFrom(t *testing.T) {
	mc := &mockS3Client{}
	a := &Adapter{client: mc, bucket: "dst-bucket"}

	mc.CopyObjectFn = func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
		if *params.Bucket != "dst-bucket" {
			t.Errorf("Wrong bucket: %s", *params.Bucket)
		}
		return &s3.CopyObjectOutput{}, nil
	}

	err := a.CopyFrom(context.Background(), "src-bucket", "src-key", "dst-key")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	mc.CopyObjectFn = func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
		return nil, fmt.Errorf("copy error")
	}
	err = a.CopyFrom(context.Background(), "src-bucket", "src-key", "dst-key")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAdapter_Upload(t *testing.T) {
	mu := &mockUploader{}
	a := &Adapter{uploader: mu, bucket: "bucket"}

	mu.UploadFn = func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
		return &manager.UploadOutput{}, nil
	}

	err := a.Upload(context.Background(), "key", strings.NewReader("body"), 4)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	mu.UploadFn = func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
		return nil, fmt.Errorf("upload error")
	}
	err = a.Upload(context.Background(), "key", strings.NewReader("body"), 4)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAdapter_Download(t *testing.T) {
	mc := &mockS3Client{}
	a := &Adapter{client: mc, bucket: "bucket"}

	mc.GetObjectFn = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader("data")),
			ContentLength: aws.Int64(4),
		}, nil
	}

	body, info, err := a.Download(context.Background(), "key")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if info.Size != 4 {
		t.Errorf("Expected size 4, got %d", info.Size)
	}
	_ = body.Close()

	mc.GetObjectFn = func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		return nil, fmt.Errorf("download error")
	}
	_, _, err = a.Download(context.Background(), "key")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAdapter_Stat(t *testing.T) {
	mc := &mockS3Client{}
	a := &Adapter{client: mc, bucket: "bucket"}

	mc.HeadObjectFn = func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(10),
		}, nil
	}

	info, err := a.Stat(context.Background(), "key")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if info.Size != 10 {
		t.Errorf("Expected size 10, got %d", info.Size)
	}

	mc.HeadObjectFn = func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
		return nil, fmt.Errorf("stat error")
	}
	_, err = a.Stat(context.Background(), "key")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAdapter_List(t *testing.T) {
	mc := &mockS3Client{}
	a := &Adapter{client: mc, bucket: "bucket"}

	mc.ListObjectsV2Fn = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
		return &s3.ListObjectsV2Output{
			Contents: []types.Object{
				{
					Key:          aws.String("file1"),
					Size:         aws.Int64(100),
					LastModified: aws.Time(time.Now()),
				},
				{
					Key:          aws.String("folder/"),
					Size:         aws.Int64(0),
					LastModified: aws.Time(time.Now()),
				},
			},
			IsTruncated: aws.Bool(false),
		}, nil
	}

	outCh, errCh := a.List(context.Background(), "folder/")

	files := 0
	for range outCh {
		files++
	}
	err := <-errCh
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	// "folder/" with size 0 should be skipped because it matches prefix and size is 0
	if files != 1 {
		t.Errorf("Expected 1 file, got %d", files)
	}

	mc.ListObjectsV2Fn = func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
		return nil, fmt.Errorf("list error")
	}
	outCh, errCh = a.List(context.Background(), "")
	for range outCh {
	}
	err = <-errCh
	if err == nil {
		t.Error("Expected error, got nil")
	}
}
