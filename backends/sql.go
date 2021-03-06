package backends

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
)

var objectFieldHintLength = 131071
var InitialPingTimeout = time.Duration(10) * time.Second
var sqlMaxExactCountRows = 10000

type sqlTableDetails struct {
	Index        int
	Name         string
	Type         string
	TypeLength   int
	Precision    int
	NativeType   string
	PrimaryKey   bool
	KeyField     bool
	Nullable     bool
	Unique       bool
	DefaultValue string
}

type additionalIndexer struct {
	ForCollection string
	Indexer       Indexer
}

type sqlTableDetailsFunc func(datasetName string, collectionName string) (*dal.Collection, error)

type SqlBackend struct {
	Backend
	Indexer
	Aggregator
	conn                      *dal.ConnectionString
	db                        *sql.DB
	indexer                   Indexer
	aggregator                map[string]Aggregator
	queryGenTypeMapping       generators.SqlTypeMapping
	queryGenNormalizerFormat  string
	listAllTablesQuery        string
	createPrimaryKeyIntFormat string
	createPrimaryKeyStrFormat string
	showTableDetailQuery      string
	refreshCollectionFunc     sqlTableDetailsFunc
	countEstimateQuery        string
	countExactQuery           string
	dropTableQuery            string
	registeredCollections     sync.Map
	knownCollections          map[string]bool
}

func NewSqlBackend(connection dal.ConnectionString) Backend {
	backend := &SqlBackend{
		conn:                &connection,
		queryGenTypeMapping: generators.DefaultSqlTypeMapping,
		dropTableQuery:      `DROP TABLE %s`,
		aggregator:          make(map[string]Aggregator),
		knownCollections:    make(map[string]bool),
	}

	backend.indexer = backend
	return backend
}

func (self *SqlBackend) Supports(features ...BackendFeature) bool {
	for _, feat := range features {
		switch feat {
		default:
			return false
		}
	}

	return true
}

func (self *SqlBackend) String() string {
	switch dbtype := self.conn.Backend(); dbtype {
	case `postgres`, `postgresql`, `psql`:
		return `postgresql`
	case `mysql`, `sqlite`:
		return dbtype
	default:
		return ``
	}
}

func (self *SqlBackend) GetConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *SqlBackend) RegisterCollection(collection *dal.Collection) {
	if collection != nil {
		self.registeredCollections.Store(collection.Name, collection)
		log.Debugf("[%v] register collection %v", self, collection.Name)
		go self.updateEstimatedCountForTable(collection)
	}
}

func (self *SqlBackend) updateEstimatedCountForTable(collection *dal.Collection) {
	if !self.conn.OptBool(`autocount`, false) {
		return
	}

	if collection != nil {
		if approx := self.countEstimateQuery; approx != `` {
			row := self.db.QueryRow(fmt.Sprintf(approx, collection.Name))
			var count int

			if err := row.Scan(&count); err == nil {
				if count > sqlMaxExactCountRows {
					collection.TotalRecords = int64(count)
					if count > 0 {
						log.Noticef("[%v] collection %v = %d record", self, collection.Name, collection.TotalRecords)
					}
					collection.TotalRecordsExact = false
					return
				}
			} else {
				log.Warningf("%v: error estimating count: %v", collection.Name, err)
			}

			if exact := self.countExactQuery; exact != `` {
				row := self.db.QueryRow(fmt.Sprintf(exact, collection.Name, sqlMaxExactCountRows))

				if err := row.Scan(&count); err == nil {
					collection.TotalRecords = int64(count)

					if count > 0 {
						log.Noticef("[%v] collection %v = %d record", self, collection.Name, collection.TotalRecords)
					}

					collection.TotalRecordsExact = true
				} else {
					log.Warningf("%v: error counting: %v", collection.Name, err)
				}
			}
		}
	}
}

func (self *SqlBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		if indexConnString.OptBool(`fallbackToBackend`, false) {
			log.Debugf("Indexer fallback to backend %T", self)

			multi := NewMultiIndex()
			multi.AddIndexer(indexer)
			multi.AddIndexer(self)

			self.indexer = multi
		} else {
			self.indexer = indexer
		}

		return nil
	} else {
		return err
	}
}

func (self *SqlBackend) Initialize() error {
	backend := self.conn.Backend()
	internalBackend := backend

	var name string
	var dsn string
	var err error

	// setup driver-specific settings
	switch self.String() {
	case `sqlite`:
		name, dsn, err = self.initializeSqlite()
	case `mysql`:
		name, dsn, err = self.initializeMysql()
	case `postgresql`:
		name, dsn, err = self.initializePostgres()
	default:
		return fmt.Errorf("Unsupported backend %q", backend)
	}

	if err != nil {
		return err
	} else if name != `` {
		internalBackend = name
	}

	// setup the database driver for use
	if db, err := sql.Open(internalBackend, dsn); err == nil {
		self.db = db
	} else {
		return err
	}

	// actually verify database connectivity at this time
	if err := self.Ping(InitialPingTimeout); err != nil {
		return err
	}

	// refresh schema cache
	if err := self.refreshAllCollections(); err != nil {
		return err
	}

	if err := self.indexer.IndexInitialize(self); err != nil {
		return err
	}

	// setup aggregators (currently this is just the SQL implementation)
	self.aggregator[``] = self

	return nil
}

func (self *SqlBackend) Ping(timeout time.Duration) error {
	if self.db == nil {
		return fmt.Errorf("Backend not initialized")
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := self.db.PingContext(ctx); err == nil {
			return nil
		} else {
			return fmt.Errorf("Backend unavailable: %v", err)
		}
	}
}

func (self *SqlBackend) Insert(name string, recordset *dal.RecordSet) error {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if tx, err := self.db.Begin(); err == nil {
			switch self.String() {
			case `mysql`:
				// disable zero-means-use-autoincrement for inserts in MySQL
				if _, err := tx.Exec(`SET sql_mode='NO_AUTO_VALUE_ON_ZERO'`); err != nil {
					defer tx.Rollback()
					return err
				}
			}

			// for each record being inserted...
			for _, record := range recordset.Records {
				if r, err := collection.MakeRecord(record); err == nil {
					record = r
				} else {
					return err
				}

				// setup query generator
				queryGen := self.makeQueryGen(collection)
				queryGen.Type = generators.SqlInsertStatement

				// add record data to query input
				for k, v := range record.Fields {
					// convert incoming values to their destination field types
					queryGen.InputData[k] = collection.ConvertValue(k, v)
				}

				// set the primary key
				if !typeutil.IsZero(record.ID) && fmt.Sprintf("%v", record.ID) != `0` {
					// convert incoming ID to it's destination field type
					queryGen.InputData[collection.IdentityField] = collection.ConvertValue(collection.IdentityField, record.ID)
				}

				// render the query into the final SQL
				if stmt, err := filter.Render(queryGen, collection.Name, filter.Null()); err == nil {
					querylog.Debugf("[%v] %s %v", self, string(stmt[:]), queryGen.GetValues())

					// execute the SQL
					if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err != nil {
						defer tx.Rollback()
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			}

			// commit transaction
			if err := tx.Commit(); err == nil {
				if search := self.WithSearch(collection); search != nil {
					if err := search.Index(collection, recordset); err != nil {
						querylog.Debugf("[%v] index error %v", self, err)
					} else {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) Exists(name string, id interface{}) bool {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if f, err := self.keyQuery(collection, id); err == nil {
			if tx, err := self.db.Begin(); err == nil {
				defer tx.Commit()

				f.Fields = []string{collection.IdentityField}
				queryGen := self.makeQueryGen(collection)

				if err := queryGen.Initialize(collection.Name); err == nil {
					if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
						querylog.Debugf("[%v] %s %v", self, string(stmt[:]), queryGen.GetValues())

						// perform query
						if rows, err := tx.Query(string(stmt[:]), queryGen.GetValues()...); err == nil {
							defer rows.Close()
							return rows.Next()
						} else {
							querylog.Debugf("[%v] query error %v", self, err)
						}
					} else {
						querylog.Debugf("[%v] query generator error %v", self, err)
					}
				} else {
					querylog.Debugf("[%v] query generator error %v", self, err)
				}
			} else {
				querylog.Debugf("[%v] transaction error %v", self, err)
			}
		} else {
			querylog.Debugf("[%v] error generating key query: %v", self, err)
		}
	} else {
		querylog.Debugf("[%v] cache error %v", self, err)
	}

	return false
}

func (self *SqlBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if f, err := self.keyQuery(collection, id); err == nil {
			f.Fields = fields
			queryGen := self.makeQueryGen(collection)

			if err := queryGen.Initialize(collection.Name); err == nil {
				if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
					querylog.Debugf("[%v] %s %v", self, string(stmt[:]), queryGen.GetValues())

					// perform query
					if rows, err := self.db.Query(string(stmt[:]), queryGen.GetValues()...); err == nil {
						defer rows.Close()

						if columns, err := rows.Columns(); err == nil {
							if rows.Next() {
								return self.scanFnValueToRecord(queryGen, collection, columns, reflect.ValueOf(rows.Scan), fields)
							} else {
								// if it doesn't exist, make sure it's not indexed
								if search := self.WithSearch(collection); search != nil {
									defer search.IndexRemove(collection, []interface{}{id})
								}

								return nil, fmt.Errorf("Record %v does not exist", id)
							}
						} else {
							return nil, err
						}
					} else {
						return nil, fmt.Errorf("retrieve error: %v", err)
					}
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *SqlBackend) Update(name string, recordset *dal.RecordSet, target ...string) error {
	var targetFilter *filter.Filter

	if len(target) > 0 {
		if f, err := filter.Parse(target[0]); err == nil {
			targetFilter = f
		} else {
			return err
		}
	}

	if collection, err := self.getCollectionFromCache(name); err == nil {
		if tx, err := self.db.Begin(); err == nil {
			// for each record being updated...
			for _, record := range recordset.Records {
				if r, err := collection.MakeRecord(record); err == nil {
					record = r
				} else {
					return err
				}

				// setup query generator
				queryGen := self.makeQueryGen(collection)
				queryGen.Type = generators.SqlUpdateStatement

				var recordUpdateFilter *filter.Filter

				// if this record was specified without a specific ID, attempt to use the broader
				// target filter (if given)
				if record.ID == `` {
					if len(target) > 0 {
						recordUpdateFilter = targetFilter
					} else {
						defer tx.Rollback()
						return fmt.Errorf("Update must target at least one record")
					}
				} else if f, err := self.keyQuery(collection, record); err == nil {
					// try to build a filter targeting this specific record
					recordUpdateFilter = f
				} else {
					defer tx.Rollback()
					return err
				}

				// add all non-ID fields to the record's Fields set
				for k, v := range record.Fields {
					if k != collection.IdentityField {
						queryGen.InputData[k] = v
					}
				}

				// generate SQL
				if stmt, err := filter.Render(queryGen, collection.Name, recordUpdateFilter); err == nil {
					querylog.Debugf("[%v] %s %v", self, string(stmt[:]), queryGen.GetValues())

					// execute SQL
					if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err != nil {
						defer tx.Rollback()
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			}

			if err := tx.Commit(); err == nil {
				if search := self.WithSearch(collection); search != nil {
					if err := search.Index(collection, recordset); err != nil {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) Delete(name string, ids ...interface{}) error {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		// remove documents from index
		if search := self.WithSearch(collection); search != nil {
			defer search.IndexRemove(collection, ids)
		}

		// TODO: need to work out how to handle DELETEs on tables with composite keys
		// f, err := self.keyQuery(collection, record)

		f := filter.New()

		f.AddCriteria(filter.Criterion{
			Field:  collection.IdentityField,
			Values: ids,
		})

		if tx, err := self.db.Begin(); err == nil {
			queryGen := self.makeQueryGen(collection)
			queryGen.Type = generators.SqlDeleteStatement

			// generate SQL
			if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
				querylog.Debugf("[%v] %s %v", self, string(stmt[:]), queryGen.GetValues())

				// execute SQL
				if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err == nil {
					if err := tx.Commit(); err == nil {
						return nil
					} else {
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			} else {
				defer tx.Rollback()
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	return self.indexer
}

func (self *SqlBackend) WithAggregator(collection *dal.Collection) Aggregator {
	if aggregator, ok := self.aggregator[collection.GetAggregatorName()]; ok {
		return aggregator
	}

	defaultAggregator, _ := self.aggregator[``]

	return defaultAggregator
}

func (self *SqlBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.registeredCollections), nil
}

func (self *SqlBackend) CreateCollection(definition *dal.Collection) error {
	// -- sqlite3
	// CREATE TABLE foo (
	//     "id"         INTEGER PRIMARY KEY ASC,
	//     "name"       TEXT NOT NULL,
	//     "enabled"    INTEGER(1),
	//     "created_at" TEXT DEFAULT CURRENT_TIMESTAMP
	// );

	// -- MySQL
	// CREATE TABLE foo (
	//     `id`         INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
	//     `name`       TEXT NOT NULL,
	//     `enabled`    TINYINT(1),
	//     `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
	// );

	// -- PostgreSQL
	// CREATE TABLE foo (
	//     "id"         BIGSERIAL PRIMARY KEY,
	//     "name"       TEXT NOT NULL,
	//     "enabled"    BOOLEAN,
	//     "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now_utc()
	// );

	// -- MS SQL Server
	// CREATE TABLE [foo] (
	//     [id]         INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	//     [name]       NVARCHAR(MAX) NOT NULL,
	//     [enabled     BIT,
	//     [created_at] [DATETIME] DEFAULT CURRENT_TIMESTAMP
	// );

	if definition.IdentityField == `` {
		definition.IdentityField = dal.DefaultIdentityField
	}

	gen := self.makeQueryGen(definition)

	stmt := fmt.Sprintf("CREATE TABLE %s (", gen.ToTableName(definition.Name))

	fields := []string{}
	values := make([]interface{}, 0)

	if definition.IdentityField != `` {
		switch definition.IdentityFieldType {
		case dal.StringType:
			fields = append(fields, fmt.Sprintf(self.createPrimaryKeyStrFormat, gen.ToFieldName(definition.IdentityField)))
		default:
			fields = append(fields, fmt.Sprintf(self.createPrimaryKeyIntFormat, gen.ToFieldName(definition.IdentityField)))
		}
	}

	for _, field := range definition.Fields {
		var def string

		// This is weird...
		//
		// So Raw fields and Object fields are stored using the same datatype (BLOB), which
		// means that when we read back the schema definition, we don't have a decisive way of
		// knowing whether that field should be treated as Raw or Object.  So we create Object fields
		// with a specific length.  This serves as a hint to us that we should treat this field as an object field.
		//
		// We could also do this with comments, but not all SQL servers necessarily support comments on
		// table schemata, so this feels more reliable in practical usage.
		//
		if field.Type == dal.ObjectType {
			field.Length = objectFieldHintLength
		}

		if nativeType, err := gen.ToNativeType(field.Type, []dal.Type{field.Subtype}, field.Length); err == nil {
			def = fmt.Sprintf("%s %s", gen.ToFieldName(field.Name), nativeType)
		} else {
			return err
		}

		if field.Required {
			def += ` NOT NULL`
		}

		if field.Unique {
			def += ` UNIQUE`
		}

		// if the default value is neither nil nor a function
		if v := field.DefaultValue; v != nil && !typeutil.IsFunction(field.DefaultValue) {
			def += fmt.Sprintf(" DEFAULT %v", gen.ToNativeValue(field.Type, []dal.Type{field.Subtype}, v))
		}

		fields = append(fields, def)
	}

	// Constraints
	// after we've added all the field definitions, append the PRIMARY KEY () constraint statement
	// with all of the key fields in the schema.
	primaryKeys := make([]string, 0)

	for _, k := range definition.KeyFields() {
		primaryKeys = append(primaryKeys, gen.ToFieldName(k.Name))
	}

	fields = append(fields, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, `, `)))

	// join all fields on "," and finish building the statement
	stmt += strings.Join(fields, `, `)
	stmt += `)`

	if tx, err := self.db.Begin(); err == nil {
		querylog.Debugf("[%v] %s %v", self, string(stmt[:]), values)

		if _, err := tx.Exec(stmt, values...); err == nil {
			defer func() {
				self.RegisterCollection(definition)

				if err := self.refreshCollectionFromDatabase(definition.Name, definition); err != nil {
					querylog.Debugf("[%v] failed to refresh collection: %v", self, err)
				}
			}()
			return tx.Commit()
		} else {
			defer tx.Rollback()
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) DeleteCollection(collectionName string) error {
	if collection, err := self.getCollectionFromCache(collectionName); err == nil {
		gen := self.makeQueryGen(collection)

		if tx, err := self.db.Begin(); err == nil {
			stmt := fmt.Sprintf(self.dropTableQuery, gen.ToTableName(collectionName))
			querylog.Debugf("[%v] %s", self, string(stmt[:]))

			if _, err := tx.Exec(stmt); err == nil {
				return tx.Commit()
			} else {
				defer tx.Rollback()
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) GetCollection(name string) (*dal.Collection, error) {
	if err := self.refreshCollectionFromDatabase(name, nil); err == nil {
		if _, ok := self.knownCollections[name]; !ok {
			return nil, dal.CollectionNotFound
		}

		if collection, err := self.getCollectionFromCache(name); err == nil {
			return collection, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *SqlBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *SqlBackend) makeQueryGen(collection *dal.Collection) *generators.Sql {
	queryGen := generators.NewSqlGenerator()
	queryGen.TypeMapping = self.queryGenTypeMapping

	if collection != nil {
		// perform string normalization on non-pk, non-key string fields
		for _, field := range collection.Fields {
			if field.Identity || field.Key {
				continue
			}

			if field.Type == dal.StringType {
				queryGen.NormalizeFields = append(queryGen.NormalizeFields, field.Name)
			}
		}

		// set the format for string normalization
		if v := self.queryGenNormalizerFormat; v != `` {
			queryGen.NormalizerFormat = v
		}
	}

	return queryGen
}

func (self *SqlBackend) scanFnValueToRecord(queryGen *generators.Sql, collection *dal.Collection, columns []string, scanFn reflect.Value, wantedFields []string) (*dal.Record, error) {
	if scanFn.Kind() != reflect.Func {
		return nil, fmt.Errorf("Can only accept a function value")
	}

	// sql.Row.Scan is strict about how we call it (e.g.: won't return results as a map),
	// so we hack...
	//
	output := make([]interface{}, len(columns))

	// put a zero-value instance of each column's type in the result array, which will
	// serve as a hint to the sql.Scan function as to how to convert the data
	for i, column := range columns {
		baseColumn := strings.Split(column, queryGen.TypeMapping.NestedFieldSeparator)[0]

		if field, ok := collection.GetField(baseColumn); ok {
			if field.DefaultValue != nil {
				output[i] = field.GetDefaultValue()
			} else if field.Required {
				output[i] = field.GetTypeInstance()
			} else {
				switch field.Type {
				case dal.StringType, dal.TimeType, dal.ObjectType:
					output[i] = sql.NullString{}

				case dal.BooleanType:
					output[i] = sql.NullBool{}

				case dal.IntType:
					output[i] = sql.NullInt64{}

				case dal.FloatType:
					output[i] = sql.NullFloat64{}

				default:
					output[i] = make([]byte, 0)
				}
			}
		}
	}

	rRowArgs := make([]reflect.Value, len(output))

	// each argument in the call to scan will be the address of the corresponding
	// item in the output array
	for i, _ := range output {
		rRowArgs[i] = reflect.ValueOf(output).Index(i).Addr()
	}

	// perform the call to the Scan() function with the correct number of "arguments"
	rRowResult := scanFn.Call(rRowArgs)

	var err error

	// the function should only return one value of type error
	if len(rRowResult) == 1 {
		v := rRowResult[0].Interface()

		if e, ok := v.(error); ok || v == nil {
			err = e
		} else {
			return nil, fmt.Errorf("row scan call returned invalid type (%T)", v)
		}
	} else {
		return nil, fmt.Errorf("invalid response from row scan call")
	}

	// this is the actual error returned from calling Scan()
	if err == nil {
		var id interface{}
		fields := make(map[string]interface{})

		// for each column in the resultset

	ColumnLoop:
		for i, column := range columns {
			nestedPath := strings.Split(column, queryGen.TypeMapping.NestedFieldSeparator)
			baseColumn := nestedPath[0]

			if field, ok := collection.GetField(baseColumn); ok {
				var value interface{}

				// convert value types as needed
				switch output[i].(type) {
				// raw byte arrays will either be strings, blobs, or binary-encoded objects
				// we need to figure out which
				case []uint8:
					value = []byte(output[i].([]uint8))

				case sql.NullString:
					v := output[i].(sql.NullString)

					if v.Valid {
						value = []byte(v.String)
					} else {
						value = nil
					}

				case sql.NullBool:
					v := output[i].(sql.NullBool)

					if v.Valid {
						value = v.Bool
					} else {
						value = nil
					}

				case sql.NullInt64:
					v := output[i].(sql.NullInt64)

					if v.Valid {
						value = v.Int64
					} else {
						value = nil
					}

				case sql.NullFloat64:
					v := output[i].(sql.NullFloat64)

					if v.Valid {
						value = v.Float64
					} else {
						value = nil
					}
				default:
					if vStr, ok := output[i].(string); ok {
						value = []byte(vStr)
					} else {
						value = output[i]
					}
				}

				// strings and raw bytes need a little love to determine exactly what they actually are
				if asBytes, ok := value.([]byte); ok {
					var dest map[string]interface{}

					switch field.Type {
					case dal.ObjectType, dal.RawType:
						if err := queryGen.ObjectTypeDecode(asBytes, &dest); err == nil {
							value = dest
						} else {
							value = asBytes
						}

					default:
						value = nil

						if vStr := string(asBytes); len(vStr) > 0 {
							var normalized []rune

							for _, r := range vStr {
								if unicode.IsGraphic(r) {
									normalized = append(normalized, r)
								}
							}

							if nS := string(normalized); nS != `` {
								value = nS
							}
						}
					}
				}

				// set the appropriate field for the dal.Record
				if v, err := field.ConvertValue(value); err == nil {
					if column == collection.IdentityField {
						id = v
					} else {
						if len(wantedFields) > 0 {
							shouldSkip := true

							for _, wantedField := range wantedFields {
								parts := strings.Split(wantedField, queryGen.TypeMapping.NestedFieldSeparator)

								if parts[0] == baseColumn {
									shouldSkip = false
									break
								}
							}

							if shouldSkip {
								continue ColumnLoop
							}

						}

						if newFields, ok := maputil.DeepSet(fields, nestedPath, v).(map[string]interface{}); ok {
							fields = newFields
						}
					}
				}
			}
		}

		record := dal.NewRecord(id).SetFields(fields)

		// do this AFTER populating the record's fields from the database
		if err := record.Populate(record, collection); err != nil {
			return nil, fmt.Errorf("error populating record: %v", err)
		}

		return record, nil
	} else {
		return nil, err
	}
}

// func (self *SqlBackend) Migrate(diff []dal.SchemaDelta) error {
// 	for _, delta := range diff {
// 		switch delta.Issue {
// 		case dal.CollectionKeyNameIssue, dal.CollectionKeyTypeIssue:
// 			return fmt.Errorf("Cannot alter key name or type for %T", self)

// 		case dal.FieldMissingIssue:
// 			// ALTER TABLE ADD COLUMN ...
// 			if collection, err := self.getCollectionFromCache(delta.Collection); err == nil {
// 				if field, ok := collection.GetField(delta.Name); ok {

// 				} else {
// 					return fmt.Errorf("Cannot add field %q: not in collection %q", delta.Name, delta.Collection)
// 				}
// 			} else {
// 				return fmt.Errorf("Cannot add field %q: %v", delta.Name, err)
// 			}

// 		case dal.FieldNameIssue:
// 			// ALTER TABLE ADD COLUMN ...
// 		case dal.FieldLengthIssue:
// 			// ALTER TABLE  ...
// 		case dal.FieldTypeIssue:
// 			// ALTER TABLE  ...
// 		case dal.FieldPropertyIssue:
// 			// ...
// 		}
// 	}

// 	return fmt.Errorf("Not Implemented")
// }

func (self *SqlBackend) refreshAllCollections() error {
	if !self.conn.OptBool(`autoregister`, DefaultAutoregister) {
		return nil
	}

	if rows, err := self.db.Query(self.listAllTablesQuery); err == nil {
		defer rows.Close()
		knownTables := make([]string, 0)

		// refresh all tables that come back from the list all tables query
		for rows.Next() {
			var tableName string

			if err := rows.Scan(&tableName); err == nil {
				knownTables = append(knownTables, tableName)

				if definitionI, ok := self.registeredCollections.Load(tableName); ok {
					definition := definitionI.(*dal.Collection)

					if err := self.refreshCollectionFromDatabase(definition.Name, definition); err != nil {
						log.Errorf("Error refreshing collection %s: %v", definition.Name, err)
					}
				} else {
					if err := self.refreshCollectionFromDatabase(tableName, nil); err != nil {
						log.Errorf("Error refreshing collection %s: %v", tableName, err)
					}
				}
			} else {
				log.Errorf("Error refreshing collection %s: %v", tableName, err)
			}
		}

		// purge from cache any tables that the list all query didn't return
		for _, key := range maputil.StringKeys(&self.registeredCollections) {
			if !sliceutil.ContainsString(knownTables, key) {
				log.Debugf("removing %v from collection cache", key)
				self.registeredCollections.Delete(key)
			}
		}

		return rows.Err()
	} else {
		return err
	}
}

func (self *SqlBackend) refreshCollectionFromDatabase(name string, definition *dal.Collection) error {
	if collection, err := self.refreshCollectionFunc(
		self.conn.Dataset(),
		name,
	); err == nil {
		if len(collection.Fields) > 0 {
			if definition != nil {
				// we've read the collection back from the database, but in the process we've lost
				// some local values that only existed on the definition itself.  we need to copy those into
				// the collection that just came back
				collection.ApplyDefinition(definition)
				self.RegisterCollection(definition)

			} else if self.conn.OptBool(`autoregister`, DefaultAutoregister) {
				self.RegisterCollection(collection)
			}

			self.knownCollections[name] = true
		}

		return nil
	} else {
		return err
	}
}

func (self *SqlBackend) getCollectionFromCache(name string) (*dal.Collection, error) {
	if registered, ok := self.registeredCollections.Load(name); ok {
		return registered.(*dal.Collection), nil
	} else {
		return nil, dal.CollectionNotFound
	}
}

func (self *SqlBackend) keyQuery(collection *dal.Collection, id interface{}) (*filter.Filter, error) {
	var ids []interface{}

	keyFields := collection.KeyFields()

	if record, ok := id.(*dal.Record); ok {
		ids = record.Keys(collection)
	} else {
		ids = sliceutil.Flatten(id)
	}

	if len(keyFields) != len(ids) {
		return nil, fmt.Errorf("Expected ID to be a slice of length %d, got %d value(s)", len(keyFields), len(ids))
	}

	idquery := map[string]interface{}{}

	for i, keyField := range keyFields {
		idquery[keyField.Name] = ids[i]
	}

	if f, err := filter.FromMap(idquery); err == nil {
		f.Limit = 1

		return f, nil
	} else {
		return nil, err
	}
}
