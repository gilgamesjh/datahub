// Copyright 2022 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/mimiro-io/datahub/internal/service/types"
)

type LegacyNamespaceAccess interface {
	// need access ot in-memory mapping
	LookupNamespaceExpansion(prefix types.Prefix) (types.URI, error)
	LookupExpansionPrefix(input types.URI) (types.Prefix, error)
}

type BadgerStore interface {
	GetDB() *badger.DB
	LookupDatasetID(datasetName string) (types.InternalDatasetID, bool)
	LookupDatasetIDs(datasetNames []string) []types.InternalDatasetID
	LookupDatasetName(internalDatasetID types.InternalDatasetID) (string, bool)

	// need access ot in-memory mapping
	IsDatasetDeleted(datasetId types.InternalDatasetID) bool
	LegacyNamespaceAccess
}
