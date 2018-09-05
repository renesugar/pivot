package backends

import (
	"fmt"
	"strings"
	"time"

	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

var querylog = log.Logger()
var stats, _ = statsd.New()
var DefaultAutoregister = false

type Backend interface {
	Initialize() error
	SetIndexer(dal.ConnectionString) error
	RegisterCollection(*dal.Collection)
	GetConnectionString() *dal.ConnectionString
	Exists(collection string, id interface{}) bool
	Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error)
	Insert(collection string, records *dal.RecordSet) error
	Update(collection string, records *dal.RecordSet, target ...string) error
	Delete(collection string, ids ...interface{}) error
	CreateCollection(definition *dal.Collection) error
	DeleteCollection(collection string) error
	ListCollections() ([]string, error)
	GetCollection(collection string) (*dal.Collection, error)
	WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer
	WithAggregator(collection *dal.Collection) Aggregator
	Flush() error
	Ping(time.Duration) error
}

var NotImplementedError = fmt.Errorf("Not Implemented")

type BackendFunc func(dal.ConnectionString) Backend

var backendMap = map[string]BackendFunc{
	`dynamodb`:   NewDynamoBackend,
	`file`:       NewFilesystemBackend,
	`fs`:         NewFilesystemBackend,
	`mongodb`:    NewMongoBackend,
	`mysql`:      NewSqlBackend,
	`postgres`:   NewSqlBackend,
	`postgresql`: NewSqlBackend,
	`psql`:       NewSqlBackend,
	`sqlite`:     NewSqlBackend,
}

func RegisterBackend(name string, fn BackendFunc) {
	backendMap[name] = fn
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	backendName := connection.Backend()
	log.Infof("Creating backend: %v", connection.String())

	if fn, ok := backendMap[backendName]; ok {
		if backend := fn(connection); backend != nil {
			return backend, nil
		} else {
			return nil, fmt.Errorf("Error occurred instantiating backend %q", backendName)
		}
	} else {
		return nil, fmt.Errorf("Unknown backend type %q", backendName)
	}
}

func InflateEmbeddedRecords(backend Backend, parent *dal.Collection, record *dal.Record, prepId func(interface{}) interface{}) error {
	record.Depth += 1

	if record.Depth > MaxRecordDepth {
		return fmt.Errorf("Embedded record expansion exceeded maximum depth of %d", MaxRecordDepth)
	} else if record.SkipEmbed {
		return nil
	}

	// for each relationship
	for _, relationship := range parent.EmbeddedCollections {
		keys := sliceutil.CompactString(sliceutil.Stringify(sliceutil.Sliceify(relationship.Keys)))

		var related *dal.Collection

		if relationship.Collection != nil {
			related = relationship.Collection
		} else if c, err := backend.GetCollection(relationship.CollectionName); c != nil {
			related = c
		} else {
			return fmt.Errorf("error in relationship %v: %v", keys, err)
		}

		var nestedFields []string

		// determine fields in final output handling
		// a. no exported fields                      -> use relationship fields
		// b. exported fields, no relationship fields -> use exported fields
		// c. both relationship and exported fields   -> relfields ∩ exported
		//
		relfields := relationship.Fields
		exported := related.ExportedFields

		if len(exported) == 0 {
			nestedFields = relfields
		} else if len(relfields) == 0 {
			nestedFields = exported
		} else {
			nestedFields = sliceutil.IntersectStrings(relfields, exported)
		}

		for _, key := range keys {
			keyBefore, _ := stringutil.SplitPair(key, `.*`)

			if nestedId := record.Get(key); nestedId != nil {
				if typeutil.IsArray(nestedId) {
					results := make([]map[string]interface{}, 0)

					for _, id := range sliceutil.Sliceify(nestedId) {
						if prepId != nil {
							id = prepId(id)
						}

						if data, err := retrieveEmbeddedRecord(backend, parent, related, id, nestedFields...); err == nil {
							results = append(results, data)
						} else {
							return err
						}
					}

					for i, result := range results {
						if len(result) > 0 {
							nestKey := strings.Replace(key, `*`, fmt.Sprintf("%d", i), 1)
							record.SetNested(nestKey, result)
						}
					}

				} else {
					if prepId != nil {
						nestedId = prepId(nestedId)
					}

					if data, err := retrieveEmbeddedRecord(backend, parent, related, nestedId, nestedFields...); err == nil {
						if len(data) > 0 {
							record.SetNested(keyBefore, data)
						}
					} else {
						return err
					}
				}
			}
		}
	}

	return nil
}

func retrieveEmbeddedRecord(backend Backend, parent *dal.Collection, related *dal.Collection, id interface{}, fields ...string) (map[string]interface{}, error) {
	if id == nil {
		return nil, nil
	}

	// retrieve the record by ID
	if record, err := backend.Retrieve(related.Name, id, fields...); err == nil {
		if data, err := related.MapFromRecord(record, fields...); err == nil {
			return data, nil
		} else if parent.AllowMissingEmbeddedRecords {
			// log.Warningf("nested(%s[%s]): %v", parent.Name, related.Name, err)
			return nil, nil
		} else {
			return nil, fmt.Errorf("nested(%s[%s]): serialization error: %v", parent.Name, related.Name, err)
		}
	} else if parent.AllowMissingEmbeddedRecords {
		// if dal.IsNotExistError(err) {
		// 	log.Warningf("nested(%s[%s]): record %v is missing", parent.Name, related.Name, id)
		// } else {
		// 	log.Warningf("nested(%s[%s]): retrieval error on %v: %v", parent.Name, related.Name, id, err)
		// }

		return nil, nil
	} else {
		return nil, fmt.Errorf("nested(%s[%s]): %v", parent.Name, related.Name, err)
	}
}
