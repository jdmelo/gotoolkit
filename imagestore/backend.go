package imagestore

import (
	"regexp"
	"sync"
)

var urlRegExp = regexp.MustCompile("sss")

type BackendStore struct {
	mu            sync.Mutex
	drivers       map[string]StoreDriver
	defaultDriver StoreDriver
}

func NewBackendStore() *BackendStore {

	return nil
}

func (bs *BackendStore) GetKnownSchemes() []string {
	bs.mu.Lock()
	bs.mu.Unlock()

	schemes := make([]string, 0)
	for k := range bs.drivers {
		schemes = append(schemes, k)
	}

	return schemes
}

func (bs *BackendStore) GetStoreFromScheme(scheme string) (StoreDriver, bool) {
	bs.mu.Lock()
	bs.mu.Unlock()

	store, ok := bs.drivers[scheme]
	if !ok {
		return nil, false
	}

	return store, true
}

func (bs *BackendStore) GetStoreFromUri(url string) (StoreDriver, bool) {
	if ok := urlRegExp.MatchString(url); !ok {
		return nil, false
	}

	// 解析url

	return nil, true
}
