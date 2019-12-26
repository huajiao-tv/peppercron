package backend

import "go.etcd.io/etcd/clientv3/concurrency"

func (s *Storage) NewMutex(key string) (*concurrency.Mutex, error) {
	session, err := concurrency.NewSession(s.Client)
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, key), nil
}
