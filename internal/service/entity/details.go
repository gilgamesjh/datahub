package entity

import (
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/mimiro-io/datahub/internal/service/namespace"
	"github.com/mimiro-io/datahub/internal/service/store"
	"github.com/mimiro-io/datahub/internal/service/types"
)

type Lookup struct {
	badger     store.BadgerStore
	namespaces namespace.Manager
}

func NewLookup(s store.BadgerStore) (Lookup, error) {
	ns := namespace.NewManager(s)
	return Lookup{s, ns}, nil
}

//Details retrieves a nested map structure with information about all datasets that contain entities with the given entity ID
// The optional datasetNames parameter allows to narrow down in which datasets the function searches
//
// The result map has the following shape
//
//
//	{
//	    "dataset1": {
//	        "changes": [
//	            "{\"id\":\"ns3:3\",\"internalId\":8,\"recorded\":1662648998417816245,\"refs\":{},\"props\":{\"ns3:name\":\"Frank\"}}"
//	        ],
//	        "latest": "{\"id\":\"ns3:3\",\"internalId\":8,\"recorded\":1662648998417816245,\"refs\":{},\"props\":{\"ns3:name\":\"Frank\"}}"
//	    },
//	    "dataset2": {
//	        "changes": [
//	            "{\"id\":\"ns3:3\",\"internalId\":8,\"recorded\":1663074960494865060,\"refs\":{},\"props\":{\"ns3:name\":\"Frank\"}}",
//	            "{\"id\":\"ns3:3\",\"internalId\":8,\"recorded\":1663075373488961084,\"refs\":{},\"props\":{\"ns3:name\":\"Frank\",\"ns4:extra\":{\"refs\":{},\"props\":{}}}}"
//	        ],
//	        "latest": "{\"id\":\"ns3:3\",\"internalId\":8,\"recorded\":1663075373488961084,\"refs\":{},\"props\":{\"ns3:name\":\"Frank\",\"ns4:extra\":{\"refs\":{},\"props\":{}}}}"
//	    },
//	}
//
func (l Lookup) Details(id string, datasetNames []string) (map[string]interface{}, error) {
	curie, err := l.asCURIE(id)
	if err != nil {
		return nil, err
	}
	b := l.badger.GetDB()

	rtxn := b.NewTransaction(false)
	defer rtxn.Discard()
	internalId, err := l.internalIdForCURIE(rtxn, curie)
	if err != nil {
		return nil, err
	}

	scope := l.badger.LookupDatasetIDs(datasetNames)
	details, err := l.loadDetails(rtxn, internalId, scope)
	if err != nil {
		return nil, err
	}
	return details, nil
}

func (l Lookup) loadDetails(rtxn *badger.Txn, internalEntityId types.InternalID, scope []types.InternalDatasetID) (map[string]interface{}, error) {
	result := map[string]interface{}{}

	opts1 := badger.DefaultIteratorOptions
	opts1.PrefetchValues = false
	entityLocatorIterator := rtxn.NewIterator(opts1)
	defer entityLocatorIterator.Close()

	entityLocatorPrefixBuffer := store.SeekEntity(internalEntityId)

	var prevValueBytes []byte
	var previousDatasetId types.InternalDatasetID = 0
	var currentDatasetId types.InternalDatasetID = 0
	partials := map[types.InternalDatasetID][]byte{}
	for entityLocatorIterator.Seek(entityLocatorPrefixBuffer); entityLocatorIterator.ValidForPrefix(entityLocatorPrefixBuffer); entityLocatorIterator.Next() {
		item := entityLocatorIterator.Item()
		key := item.Key()

		currentDatasetId = types.InternalDatasetID(binary.BigEndian.Uint32(key[10:]))

		// check if dataset has been deleted, or must be excluded
		datasetDeleted := l.badger.IsDatasetDeleted(currentDatasetId)
		datasetIncluded := len(scope) == 0 // no specified datasets means no restriction - all datasets are allowed
		if !datasetIncluded {
			for _, id := range scope {
				if id == currentDatasetId {
					datasetIncluded = true
					break
				}
			}
		}
		if datasetDeleted || !datasetIncluded {
			continue
		}

		if previousDatasetId != 0 {
			if currentDatasetId != previousDatasetId {
				partials[previousDatasetId] = prevValueBytes
			}
		}

		previousDatasetId = currentDatasetId

		// fixme: pre alloc big ish buffer once and use value size
		prevValueBytes, _ = item.ValueCopy(nil)
	}

	if previousDatasetId != 0 {
		partials[previousDatasetId] = prevValueBytes
	}

	for internalDatasetId, entityBytes := range partials {
		n, ok := l.badger.LookupDatasetName(internalDatasetId)
		if !ok {
			result[fmt.Sprintf("%v", internalDatasetId)] = "UNEXPECTED: dataset name not found"
		} else {
			result[n] = map[string]interface{}{
				"latest":  string(entityBytes),
				"changes": l.loadChanges(rtxn, internalEntityId, internalDatasetId),
			}
		}
	}
	return result, nil

}

func (l Lookup) loadChanges(rtxn *badger.Txn, internalEntityID types.InternalID, internalDatasetID types.InternalDatasetID) []string {
	seekPrefix := store.SeekEntityChanges(internalDatasetID, internalEntityID)
	iteratorOptions := badger.DefaultIteratorOptions
	iteratorOptions.PrefetchValues = true
	iteratorOptions.PrefetchSize = 1
	iteratorOptions.Prefix = seekPrefix
	entityChangesIterator := rtxn.NewIterator(iteratorOptions)
	defer entityChangesIterator.Close()

	result := make([]string, 0)
	for entityChangesIterator.Rewind(); entityChangesIterator.ValidForPrefix(seekPrefix); entityChangesIterator.Next() {
		item := entityChangesIterator.Item()
		change, _ := item.ValueCopy(nil)
		result = append(result, string(change))
	}
	return result
}
