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
	"encoding/binary"
	"github.com/mimiro-io/datahub/internal/service/types"

	"github.com/mimiro-io/datahub/internal/server"
)

func SeekChanges(datasetID types.InternalDatasetID, since types.DatasetOffset) []byte {
	searchBuffer := make([]byte, 14)
	binary.BigEndian.PutUint16(searchBuffer, server.DATASET_ENTITY_CHANGE_LOG)
	binary.BigEndian.PutUint32(searchBuffer[2:], uint32(datasetID))
	binary.BigEndian.PutUint64(searchBuffer[6:], uint64(since))
	return searchBuffer
}

func SeekEntityChanges(datasetID types.InternalDatasetID, entityID types.InternalID) []byte {
	entityIdBuffer := make([]byte, 14)
	binary.BigEndian.PutUint16(entityIdBuffer, server.ENTITY_ID_TO_JSON_INDEX_ID)
	binary.BigEndian.PutUint64(entityIdBuffer[2:], uint64(entityID))
	binary.BigEndian.PutUint32(entityIdBuffer[10:], uint32(datasetID))
	return entityIdBuffer
}

func SeekDataset(datasetID types.InternalDatasetID) []byte {
	searchBuffer := make([]byte, 6)
	binary.BigEndian.PutUint16(searchBuffer, server.DATASET_ENTITY_CHANGE_LOG)
	binary.BigEndian.PutUint32(searchBuffer[2:], uint32(datasetID))
	return searchBuffer
}

func SeekEntity(intenalEntityId types.InternalID) []byte {
	entityLocatorPrefixBuffer := make([]byte, 10)
	binary.BigEndian.PutUint16(entityLocatorPrefixBuffer, server.ENTITY_ID_TO_JSON_INDEX_ID)
	binary.BigEndian.PutUint64(entityLocatorPrefixBuffer[2:], uint64(intenalEntityId))
	return entityLocatorPrefixBuffer
}

func GetCurieKey(curie types.CURIE) []byte {
	curieAsBytes := []byte(curie)
	buf := make([]byte, len(curieAsBytes)+2)
	binary.BigEndian.PutUint16(buf, server.URI_TO_ID_INDEX_ID)
	copy(buf[2:], curieAsBytes)
	return buf
}
