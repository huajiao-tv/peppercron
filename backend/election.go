package backend

import "go.etcd.io/etcd/clientv3/concurrency"

func (s *Storage) NewElection(key string) (*concurrency.Election, error) {
	session, err := concurrency.NewSession(s.Client)
	if err != nil {
		return nil, err
	}
	return concurrency.NewElection(session, key), nil
}
