package util

import (
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/timestamp"
)

// GetCurrentTime 获取当前时间，返回 GRPC 使用
func GetCurrentTime() *timestamp.Timestamp {
	now := time.Now()
	return ToTimeStamp(now)
}

// ToTimeStamp 转换 time.Time 为 GRPC 时间
func ToTimeStamp(t time.Time) *timestamp.Timestamp {
	ts, err := ptypes.TimestampProto(t)
	if err != nil {
		log.Println(err)
	}
	return ts
}

// ToTime 转换 GRPC 时间为 time.Time
func ToTime(ts *timestamp.Timestamp) time.Time {
	t, err := ptypes.Timestamp(ts)
	if err != nil {
		log.Println(err)
	}
	return t
}

// ToDuration 转换 GRPC Duration 为 time.Duration
func ToDuration(d *duration.Duration) time.Duration {
	dur, err := ptypes.Duration(d)
	if err != nil {
		log.Println(err)
	}
	return dur
}
