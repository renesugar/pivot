package pivot

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghodss/yaml"
)

var MonitorCheckInterval = time.Duration(10) * time.Second
var NetrcFile = ``

// Create a new backend from the given Connection String and options.
func NewDatabaseWithOptions(connection string, options backends.ConnectOptions) (backends.Backend, error) {
	connections := strings.Split(connection, `|`)
	connstrings := make([]dal.ConnectionString, 0)
	createdBackends := make([]backends.Backend, 0)

	var finalBackend backends.Backend

	// parse all connection strings
	for _, connstring := range connections {
		if cs, err := dal.ParseConnectionString(connstring); err == nil {
			connstrings = append(connstrings, cs)
		} else {
			return nil, err
		}
	}

	// make backends for each connection string
	for _, cs := range connstrings {
		if NetrcFile != `` {
			if err := cs.LoadCredentialsFromNetrc(NetrcFile); err != nil {
				return nil, err
			}
		}

		if backend, err := backends.MakeBackend(cs); err == nil {
			createdBackends = append(createdBackends, backend)
		} else {
			return nil, err
		}
	}

	// create federated backend if n>1
	switch len(createdBackends) {
	case 0:
		return nil, fmt.Errorf("No backends could be created from the given connection string")
	case 1:
		finalBackend = createdBackends[0]
	default:
		finalBackend = backends.NewFederatedBackend(createdBackends)
		log.Debugf("Using federated backend across %d connections", len(createdBackends))
	}

	// set indexer
	if options.Indexer != `` {
		if ics, err := dal.ParseConnectionString(options.Indexer); err == nil {
			if NetrcFile != `` {
				if err := ics.LoadCredentialsFromNetrc(NetrcFile); err != nil {
					return nil, err
				}
			}

			if err := finalBackend.SetIndexer(ics); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	if !options.SkipInitialize {
		if err := finalBackend.Initialize(); err != nil {
			return nil, err
		}
	}

	return finalBackend, nil
}

// Create a new backend from the given Connection String, using the default options.
func NewDatabase(connection string) (backends.Backend, error) {
	return NewDatabaseWithOptions(connection, backends.ConnectOptions{})
}

// Load schema definitions from the given JSON or YAML file.
func LoadSchemataFromFile(filename string) ([]*dal.Collection, error) {
	if file, err := os.Open(filename); err == nil {
		var collections []*dal.Collection

		switch ext := path.Ext(filename); ext {
		case `.json`:
			if err := json.NewDecoder(file).Decode(&collections); err != nil {
				return nil, fmt.Errorf("decode error: %v", err)
			}

		case `.yml`, `.yaml`:
			if data, err := ioutil.ReadAll(file); err == nil {
				if err := yaml.Unmarshal(data, &collections); err != nil {
					return nil, fmt.Errorf("decode error: %v", err)
				}
			} else {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("Unrecognized file extension %s", ext)
		}

		return collections, nil
	} else {
		return nil, err
	}
}
