package backends

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/gocql/gocql"
)

type CassandraBackend struct {
	Backend
	cs         dal.ConnectionString
	db         *gocql.ClusterConfig
	session    *gocql.Session
	tableCache sync.Map
	indexer    Indexer
}

func NewCassandraBackend(connection dal.ConnectionString) Backend {
	return &CassandraBackend{
		cs: connection,
	}
}

func (self *CassandraBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *CassandraBackend) Ping(timeout time.Duration) error {
	if self.db == nil {
		return fmt.Errorf("Backend not initialized")
	} else {
		// do something to verify connectivity
	}

	return nil
}

func (self *CassandraBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *CassandraBackend) Initialize() error {
	hosts := strings.Split(self.cs.Host(), `,`)

	self.db = gocql.NewCluster(hosts...)
	self.db.Keyspace = strings.TrimPrefix(self.cs.Dataset(), `/`)

	if session, err := self.db.CreateSession(); err == nil {
		self.session = session
	} else {
		return err
	}

	// retrieve each table once as a cache warming mechanism
	if keyspace, err := self.session.KeyspaceMetadata(self.db.Keyspace); err == nil {
		for tableName, metadata := range keyspace.Tables {
			if collection, err := self.collectionFromMetadata(tableName, metadata); err == nil {
				self.RegisterCollection(collection)
			} else {
				return err
			}
		}
	} else {
		return err
	}

	if self.indexer != nil {
		if err := self.indexer.IndexInitialize(self); err != nil {
			return err
		}
	}

	return nil
}

func (self *CassandraBackend) RegisterCollection(definition *dal.Collection) {
	self.tableCache.Store(definition.Name, definition)
}

func (self *CassandraBackend) Exists(name string, id interface{}) bool {
	return false
}

func (self *CassandraBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {

}

func (self *CassandraBackend) Insert(name string, records *dal.RecordSet) error {

}

func (self *CassandraBackend) Update(name string, records *dal.RecordSet, target ...string) error {

}

func (self *CassandraBackend) Delete(name string, ids ...interface{}) error {

	return nil
}

func (self *CassandraBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *CassandraBackend) DeleteCollection(name string) error {

}

func (self *CassandraBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.tableCache), nil
}

func (self *CassandraBackend) GetCollection(name string) (*dal.Collection, error) {
	return self.cacheTable(name)
}

func (self *CassandraBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	// if this is a query we _can_ handle, then use ourself as the indexer
	if len(filters) > 0 {
		if err := self.validateFilter(collection, filters[0]); err == nil {
			return self
		}
	}

	return self.indexer
}

func (self *CassandraBackend) WithAggregator(collection *dal.Collection) Aggregator {
	if self.indexer != nil {
		if agg, ok := self.indexer.(Aggregator); ok {
			return agg
		}
	}

	return nil
}

func (self *CassandraBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *CassandraBackend) cacheTable(name string) (*dal.Collection, error) {
	if table, err := self.db.Table(name).Describe().Run(); err == nil {
		if collectionI, ok := self.tableCache.Load(name); ok {
			return collectionI.(*dal.Collection), nil
		} else {
			collection := &dal.Collection{
				Name:              table.Name,
				IdentityField:     table.HashKey,
				IdentityFieldType: self.toDalType(table.HashKeyType),
			}

			collection.AddFields(dal.Field{
				Name:     table.HashKey,
				Identity: true,
				Key:      true,
				Required: true,
				Type:     self.toDalType(table.HashKeyType),
			})

			if rangeKey := table.RangeKey; rangeKey != `` {
				collection.AddFields(dal.Field{
					Name:     rangeKey,
					Key:      true,
					Required: true,
					Type:     self.toDalType(table.RangeKeyType),
				})
			}

			self.tableCache.Store(name, collection)
			return collection, nil
		}
	} else {
		return nil, err
	}
}

func (self *CassandraBackend) toRecordKeyFilter(collection *dal.Collection, id interface{}) (*filter.Filter, *dal.Field, error) {
	var rangeKey *dal.Field
	var hashValue interface{}
	var rangeValue interface{}
	var allowMissingRangeKey bool

	// detect if the collection has a range key (i.e.: sort key)
	if _, ok := collection.GetFirstNonIdentityKeyField(); !ok {
		allowMissingRangeKey = true
	}

	for _, field := range collection.Fields {
		if f := field; f.Key {
			rangeKey = &f
		}
	}

	// at least the identity field must have been found
	if collection.IdentityField == `` {
		return nil, nil, fmt.Errorf("No identity field found in collection %v", collection.Name)
	}

	flt := filter.New()
	flt.Limit = 1
	flt.IdentityField = collection.IdentityField

	// if the rangeKey exists, then the id value must be a slice/array containing both parts
	if typeutil.IsArray(id) {
		if v, ok := sliceutil.At(id, 0); ok && v != nil {
			hashValue = v
		}
	} else {
		hashValue = id
	}

	if hashValue != nil {
		flt.AddCriteria(filter.Criterion{
			Field:  collection.IdentityField,
			Type:   collection.IdentityFieldType,
			Values: []interface{}{hashValue},
		})

		if rangeKey != nil {
			if typeutil.IsArray(id) {
				if v, ok := sliceutil.At(id, 1); ok && v != nil {
					rangeValue = v
				}

				flt.AddCriteria(filter.Criterion{
					Type:   rangeKey.Type,
					Field:  rangeKey.Name,
					Values: []interface{}{rangeValue},
				})

			} else if !allowMissingRangeKey {
				return nil, nil, fmt.Errorf("Second ID component must not be nil")
			}
		}

		return flt, rangeKey, nil
	} else {
		return nil, nil, fmt.Errorf("First ID component must not be nil")
	}
}

func (self *CassandraBackend) collectionFromMetadata(name string, metadata *gocql.TableMetadata) (*dal.Collection, error) {
	collection := dal.NewCollection(name)

	if len(metadata.PartitionKey) > 0 {
		collection.IdentityField = metadata.PartitionKey[0].Name
		collection.IdentityFieldType = self.fromCassandraType(metadata.PartitionKey[0].Type.Type())
	}

	for _, clusterKey := range metadata.ClusteringColumns {
		collection.AddFields(dal.Field{
			Name: clusterKey.Name,
			Type: self.fromCassandraType(clusterKey.Type.Type()),
			Key:  true,
		})
	}

	for _, columnName := range metadata.OrderedColumns {
		if column, ok := metadata.Columns[columnName]; ok {
			collection.AddFields(dal.Field{
				Name: column.Name,
				Type: self.fromCassandraType(column.Type.Type()),
			})
		}
	}

	return collection, nil
}

func (self *CassandraBackend) fromCassandraType(ctype gocql.Type) dal.Type {
	switch ctype {
	case gocql.TypeBigInt, gocql.TypeInt, gocql.TypeVarint, gocql.TypeTinyInt, gocql.TypeSmallInt, gocql.TypeCounter:
		return dal.IntType
	case gocql.TypeDecimal, gocql.TypeDouble, gocql.TypeFloat:
		return dal.FloatType
	case gocql.TypeBoolean:
		return dal.BooleanType
	case gocql.TypeTimestamp, gocql.TypeDate:
		return dal.TimeType
	case gocql.TypeMap:
		return dal.ObjectType
	case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar:
		return dal.StringType
	default:
		return dal.RawType
	}
}
