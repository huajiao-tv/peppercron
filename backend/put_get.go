package backend

import (
	"context"
	"time"

	v3 "go.etcd.io/etcd/clientv3"
)

const (
	DialTimeout  = 5 * time.Second
	ReadTimeout  = 5 * time.Second
	WriteTimeout = 5 * time.Second

	RetryTimes = 3
)

type Storage struct {
	*v3.Client
}

func NewStorage(endPoints []string, user, password string) (*Storage, error) {
	c, err := v3.New(v3.Config{
		Endpoints:   endPoints,
		DialTimeout: DialTimeout,
		Username:    user,
		Password:    password,
	})
	if err != nil {
		return nil, err
	}
	return &Storage{c}, nil
}

func (s *Storage) Put(ctx context.Context, key, value string, opts ...v3.OpOption) (resp *v3.PutResponse, err error) {
	for i := 0; i < RetryTimes; i++ {
		ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
		resp, err = s.Client.Put(ctx, key, value, opts...)
		cancel()
		if err == nil {
			return
		}
	}
	return
}

func (s *Storage) Get(ctx context.Context, key string, opts ...v3.OpOption) (resp *v3.GetResponse, err error) {
	for i := 0; i < RetryTimes; i++ {
		ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
		resp, err = s.Client.Get(ctx, key, opts...)
		cancel()
		if err == nil {
			return
		}
	}
	return
}

func (s *Storage) Delete(ctx context.Context, key string, opts ...v3.OpOption) (resp *v3.DeleteResponse, err error) {
	for i := 0; i < RetryTimes; i++ {
		ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
		resp, err = s.Client.Delete(ctx, key, opts...)
		cancel()
		if err == nil {
			return
		}
	}
	return
}
