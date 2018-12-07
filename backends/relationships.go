package backends

import (
	"fmt"
	"strings"

	"github.com/ghetzel/go-stockutil/log"
	// "github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	// "github.com/ghetzel/pivot/v3/filter"
)

type DeferredRecord struct {
	Backend           Backend     `json:"-"`
	Resolved          bool        `json:"-"`
	Deferred          bool        `json:"_deferred"`
	CollectionName    string      `json:"_collection"`
	IdentityFieldName string      `json:"_identity_field_name"`
	ID                interface{} `json:"_id"`
	Key               string      `json:"_key"`
	Fields            []string    `json:"_fields,omitempty"`
	expanded          interface{}
}

func (self *DeferredRecord) CacheKey(id interface{}) string {
	base := fmt.Sprintf("%v:%v", self.Backend.String(), self.CollectionName)
	base += fmt.Sprintf(":%v", id)

	if len(self.Fields) > 0 {
		return base + `@` + strings.Join(self.Fields, `,`)
	} else {
		return base
	}
}

func (self *DeferredRecord) Resolve() error {
	if !self.Resolved {
		self.Resolved = true
		if record, err := self.Backend.Retrieve(self.CollectionName, self.ID, self.Fields...); err == nil {
			data := make(map[string]interface{})

			for k, v := range record.Fields {
				if len(self.Fields) == 0 || sliceutil.ContainsString(self.Fields, k) {
					data[k] = v
				}
			}

			data[self.IdentityFieldName] = record.ID
			self.expanded = data
		} else {
			return err
		}
	}

	return nil
}

// func (self *DeferredRecord) MarshalJSON() ([]byte, error) {
// 	if err := self.Resolve(); err == nil {
// 		return json.Marshal(self.expanded)
// 	} else {
// 		return nil, err
// 	}
// }

func (self *DeferredRecord) String() string {
	return self.CacheKey(self.ID)
}

func PopulateRelationships(backend Backend, parent *dal.Collection, record *dal.Record, prepId func(interface{}) interface{}, requestedFields ...string) error { // for each relationship
	skipKeys := make([]string, 0)

	if embed, ok := backend.(*EmbeddedRecordBackend); ok {
		skipKeys = embed.SkipKeys
	}

	for _, relationship := range parent.EmbeddedCollections {
		keys := sliceutil.CompactString(sliceutil.Stringify(sliceutil.Sliceify(relationship.Keys)))

		// if we're supposed to skip certain keys, and this is one of them
		if len(skipKeys) > 0 && sliceutil.ContainsAnyString(skipKeys, keys...) {
			log.Debugf("explicitly skipping %+v", keys)
			continue
		}

		var related *dal.Collection

		if relationship.Collection != nil {
			related = relationship.Collection
		} else if c, err := backend.GetCollection(relationship.CollectionName); c != nil {
			related = c
		} else {
			return fmt.Errorf("error in relationship %v: %v", keys, err)
		}

		if related.Name == parent.Name {
			log.Debugf("not descending into %v to avoid loop", related.Name)
			continue
		}

		var nestedFields []string

		// determine fields in final output handling
		// a. no exported fields                      -> use relationship fields
		// b. exported fields, no relationship fields -> use exported fields
		// c. both relationship and exported fields   -> relfields ∩ exported
		//
		relfields := relationship.Fields
		exported := related.ExportedFields
		reqfields := make([]string, len(requestedFields))
		copy(reqfields, requestedFields)

		if len(exported) == 0 {
			nestedFields = relfields
		} else if len(relfields) == 0 {
			nestedFields = exported
		} else {
			nestedFields = sliceutil.IntersectStrings(relfields, exported)
		}

		// split the nested subfields
		for i, rel := range reqfields {
			if first, last := stringutil.SplitPair(rel, `:`); sliceutil.ContainsString(keys, first) {
				reqfields[i] = last
			} else {
				reqfields[i] = ``
			}
		}

		reqfields = sliceutil.CompactString(reqfields)

		// type DeferredRecord struct {
		// 	Original          interface{}
		// 	Backend           Backend
		// 	IdentityFieldName string
		// 	CollectionName    string
		// 	ID                interface{}
		// 	Key               string
		// 	Fields            []string
		// }

		// finally, further constraing the fieldset by those fields being requested
		if len(nestedFields) == 0 {
			nestedFields = reqfields
		} else if len(reqfields) > 0 {
			nestedFields = sliceutil.IntersectStrings(nestedFields, reqfields)
		}

		for _, key := range keys {
			if strings.Contains(key, `*`) {
				keyBefore, keyAfter := stringutil.SplitPair(key, `.*`)

				parentArray := record.Get(keyBefore)

				if typeutil.IsArray(parentArray) {
					for i, el := range sliceutil.Sliceify(parentArray) {
						abskey := fmt.Sprintf("%v.%d%v", keyBefore, i, keyAfter)

						if typeutil.IsScalar(el) && el != nil {
							record.SetNested(abskey, &DeferredRecord{
								Deferred:          true,
								Backend:           backend,
								IdentityFieldName: related.GetIdentityFieldName(),
								CollectionName:    related.Name,
								ID:                el,
								Key:               abskey,
								Fields:            nestedFields,
							})
						}
					}
				}
			} else {
				el := record.Get(key)

				if typeutil.IsScalar(el) && el != nil {
					record.SetNested(key, &DeferredRecord{
						Deferred:          true,
						Backend:           backend,
						IdentityFieldName: related.GetIdentityFieldName(),
						CollectionName:    related.Name,
						ID:                el,
						Key:               key,
						Fields:            nestedFields,
					})
				}
			}
		}
	}

	return nil
}

func ResolveDeferredRecords(cache map[string]interface{}, records ...*dal.Record) error {

	return nil
}
