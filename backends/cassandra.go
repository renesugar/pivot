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
	"github.com/guregu/dynamo"
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
	if flt, query, err := self.getSingleRecordQuery(name, id, fields...); err == nil {
		output := make(map[string]interface{})

		if err := query.One(&output); err == nil {
			record := dal.NewRecord(output[flt.IdentityField])
			delete(output, flt.IdentityField)
			record.SetFields(output)

			return record, nil
		} else if err == dynamo.ErrNotFound {
			return nil, fmt.Errorf("Record %v does not exist", id)
		} else if err == dynamo.ErrTooMany {
			return nil, fmt.Errorf("Too many records found for ID %v", id)
		} else {
			return nil, fmt.Errorf("query error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *CassandraBackend) Insert(name string, records *dal.RecordSet) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, true)
	} else {
		return err
	}
}

func (self *CassandraBackend) Update(name string, records *dal.RecordSet, target ...string) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, false)
	} else {
		return err
	}
}

func (self *CassandraBackend) Delete(name string, ids ...interface{}) error {
	for _, id := range ids {
		if op, err := self.getSingleRecordDelete(name, id); err == nil {
			op.Run()

			// TODO: call IndexRemove here
		} else {
			return err
		}
	}

	return nil
}

func (self *CassandraBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *CassandraBackend) DeleteCollection(name string) error {
	if _, err := self.GetCollection(name); err == nil {
		if err := self.db.Table(name).DeleteTable().Run(); err == nil {
			self.tableCache.Delete(name)
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
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

func (self *CassandraBackend) toDalType(t dynamo.KeyType) dal.Type {
	switch t {
	case dynamo.BinaryType:
		return dal.RawType
	case dynamo.NumberType:
		return dal.FloatType
	default:
		return dal.StringType
	}
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

func (self *CassandraBackend) getSingleRecordQuery(name string, id interface{}, fields ...string) (*filter.Filter, *dynamo.Query, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, rangeKey, err := self.toRecordKeyFilter(collection, id); err == nil {
			if hashKeyValue, ok := flt.GetIdentityValue(); ok {
				query := self.db.Table(collection.Name).Get(flt.IdentityField, hashKeyValue)

				query.Consistent(self.cs.OptBool(`readsConsistent`, true))
				query.Limit(int64(flt.Limit))

				querylog.Debugf("[%T] retrieve: %v %v", self, collection.Name, id)

				if rangeKey != nil {
					if rV, ok := flt.GetValues(rangeKey.Name); ok {
						query.Range(rangeKey.Name, dynamo.Equal, rV...)
					} else {
						return nil, nil, fmt.Errorf("Could not determine range key value")
					}
				}

				return flt, query, nil
			} else {
				return nil, nil, fmt.Errorf("Could not determine hash key value")
			}
		} else {
			return nil, nil, fmt.Errorf("filter create error: %v", err)
		}
	} else {
		return nil, nil, err
	}
}

func (self *CassandraBackend) getSingleRecordDelete(name string, id interface{}) (*dynamo.Delete, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, rangeKey, err := self.toRecordKeyFilter(collection, id); err == nil {
			if hashKeyValue, ok := flt.GetIdentityValue(); ok {
				deleteOp := self.db.Table(collection.Name).Delete(flt.IdentityField, hashKeyValue)

				if rangeKey != nil {
					if rV, ok := flt.GetValues(rangeKey.Name); ok && len(rV) > 0 {
						deleteOp.Range(rangeKey.Name, rV[0])
					} else {
						return nil, fmt.Errorf("Could not determine range key value")
					}
				}

				return deleteOp, nil
			} else {
				return nil, fmt.Errorf("Could not determine hash key value")
			}
		} else {
			return nil, fmt.Errorf("filter create error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *CassandraBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {
		item := make(map[string]interface{})

		for k, v := range record.Fields {
			if typeutil.IsZero(v) {
				continue
			}

			switch v.(type) {
			case *time.Time:
				item[k] = v.(*time.Time).Format(time.RFC3339Nano)
			case time.Time:
				item[k] = v.(time.Time).Format(time.RFC3339Nano)
			default:
				item[k] = v
			}
		}

		item[collection.IdentityField] = record.ID

		op := self.db.Table(collection.Name).Put(item)

		if isCreate {
			expr := []string{`attribute_not_exists($)`}

			exprValues := []interface{}{record.ID}

			if rangeKey, ok := collection.GetFirstNonIdentityKeyField(); ok {
				expr = append(expr, `attribute_not_exists($)`)

				if v := record.Get(rangeKey.Name); v != nil {
					exprValues = append(exprValues, v)
				} else {
					return fmt.Errorf("Cannot create record: missing range key")
				}
			}

			op.If(strings.Join(expr, ` AND `), exprValues...)
		}

		if err := op.Run(); err != nil {
			return err
		}
	}

	if !collection.SkipIndexPersistence {
		if search := self.WithSearch(collection); search != nil {
			if err := search.Index(collection, records); err != nil {
				return err
			}
		}
	}

	return nil
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
