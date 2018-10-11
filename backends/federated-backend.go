package backends

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type FederatedBackend struct {
	backends          []Backend
	collectionIndices map[string]int
}

func NewFederatedBackend(backends []Backend) *FederatedBackend {
	return &FederatedBackend{
		backends:          backends,
		collectionIndices: make(map[string]int),
	}
}

func (self *FederatedBackend) Initialize() error {
	self.collectionIndices = make(map[string]int)

	for _, backend := range self.backends {
		if err := backend.Initialize(); err != nil {
			return err
		}
	}

	return self.refreshCollectionIndices()
}

func (self *FederatedBackend) Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error) {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.Retrieve(collection, id, fields...)
	} else {
		return nil, err
	}
}

// passthrough the remaining functions to fulfill the Backend interface
// -------------------------------------------------------------------------------------------------
func (self *FederatedBackend) Exists(collection string, id interface{}) bool {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.Exists(collection, id)
	} else {
		return false
	}
}

func (self *FederatedBackend) SetIndexer(cs dal.ConnectionString) error {
	for _, backend := range self.backends {
		if err := backend.SetIndexer(cs); err != nil {
			return err
		}
	}

	return nil
}

func (self *FederatedBackend) RegisterCollection(c *dal.Collection) {
	if c.BackendID < len(self.backends) {
		self.backends[c.BackendID].RegisterCollection(c)
		self.refreshCollectionIndices()
	}
}

func (self *FederatedBackend) GetConnectionString() *dal.ConnectionString {
	if len(self.backends) > 0 {
		return self.backends[0].GetConnectionString()
	} else {
		return nil
	}
}

func (self *FederatedBackend) Insert(collection string, records *dal.RecordSet) error {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.Insert(collection, records)
	} else {
		return err
	}
}

func (self *FederatedBackend) Update(collection string, records *dal.RecordSet, target ...string) error {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.Update(collection, records, target...)
	} else {
		return err
	}
}

func (self *FederatedBackend) Delete(collection string, ids ...interface{}) error {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.Delete(collection, ids...)
	} else {
		return err
	}
}

func (self *FederatedBackend) CreateCollection(definition *dal.Collection) error {
	if backend, err := self.backendForCollection(definition.Name); err == nil {
		return backend.CreateCollection(definition)
	} else {
		return err
	}
}

func (self *FederatedBackend) DeleteCollection(collection string) error {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.DeleteCollection(collection)
	} else {
		return err
	}
}

func (self *FederatedBackend) ListCollections() ([]string, error) {
	collections := maputil.StringKeys(self.collectionIndices)
	sort.Strings(collections)

	return collections, nil
}

func (self *FederatedBackend) GetCollection(collection string) (*dal.Collection, error) {
	if backend, err := self.backendForCollection(collection); err == nil {
		return backend.GetCollection(collection)
	} else {
		return nil, err
	}
}

func (self *FederatedBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	if backend, err := self.backendForCollection(collection.Name); err == nil {
		return backend.WithSearch(collection, filters...)
	} else {
		return nil
	}
}

func (self *FederatedBackend) WithAggregator(collection *dal.Collection) Aggregator {
	if backend, err := self.backendForCollection(collection.Name); err == nil {
		return backend.WithAggregator(collection)
	} else {
		return nil
	}
}

func (self *FederatedBackend) Flush() error {
	for _, backend := range self.backends {
		if err := backend.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (self *FederatedBackend) Ping(d time.Duration) error {
	for _, backend := range self.backends {
		if err := backend.Ping(d); err != nil {
			return err
		}
	}

	return nil
}

func (self *FederatedBackend) String() string {
	return strings.Join(sliceutil.Stringify(self.backends), `|`)
}

func (self *FederatedBackend) Supports(feature ...BackendFeature) bool {
	for _, backend := range self.backends {
		if !backend.Supports(feature...) {
			return false
		}
	}

	return true
}

func (self *FederatedBackend) backendForCollection(name string) (Backend, error) {
	if id, ok := self.collectionIndices[name]; ok {
		if id < len(self.backends) {
			return self.backends[id], nil
		} else {
			return nil, fmt.Errorf("Backend index %d is out of range", id)
		}
	} else {
		return nil, fmt.Errorf("No backend found for collection %q", name)
	}
}

func (self *FederatedBackend) refreshCollectionIndices() error {
	indices := make(map[string]int)

	for i, backend := range self.backends {
		if collections, err := backend.ListCollections(); err == nil {
			for _, name := range collections {
				if ci, ok := indices[name]; !ok {
					indices[name] = i
				} else {
					log.Warningf("Cannot register collection to backend %d, already belongs to backend %d", i, ci)
				}
			}
		} else {
			return err
		}
	}

	self.collectionIndices = indices
	return nil
}
