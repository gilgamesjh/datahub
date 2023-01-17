package jobs

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
)

// SyncJobState used to capture the state of a running job
type SyncJobState struct {
	ID                 string `json:"id"`
	ContinuationToken  string `json:"token"`
	LastRunCompletedOk bool   `json:"lastrunok"`
	LastRunError       string `json:"lastrunerror"`
}

type SyncState interface {
	// Get will retrieve the sync state for a given job task, if not found it will return an empty token
	Get(jobId scheduler.JobId, taskId string) (*SyncJobState, error)
	Update(jobId scheduler.JobId, taskId string, state *SyncJobState) error
	ResetAll(jobId scheduler.JobId) error
	Reset(jobId scheduler.JobId, taskId string) error
}

var _ SyncState = (*DataHubStoreSyncState)(nil)

type DataHubStoreSyncState struct {
	store *server.Store
}

func NewDataHubSyncState(store *server.Store) SyncState {
	return &DataHubStoreSyncState{store: store}
}

func (s *DataHubStoreSyncState) Get(jobId scheduler.JobId, taskId string) (*SyncJobState, error) {
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	state, err := server.GetObject[SyncJobState](s.store, server.JOB_DATA_INDEX, id)
	if err != nil {
		return nil, err
	}

	if state == nil {
		// for upgrade purposes, if not found with a full id, maybe needs upgrading
		found, _ := server.GetObject[SyncJobState](s.store, server.JOB_DATA_INDEX, taskId)
		if found != nil {
			err := s.Update(jobId, taskId, found)
			if err == nil {
				_ = s.store.DeleteObject(server.JOB_DATA_INDEX, taskId)
			}
		}
		return &SyncJobState{ID: id}, nil
	}
	return state, nil
}

func (s *DataHubStoreSyncState) Update(jobId scheduler.JobId, taskId string, state *SyncJobState) error {
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	return s.store.StoreObject(server.JOB_DATA_INDEX, id, state)
}

func (s *DataHubStoreSyncState) ResetAll(jobId scheduler.JobId) error {
	items := make([]*SyncJobState, 0)
	indexBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(indexBytes, uint16(server.JOB_DATA_INDEX))
	indexBytes = append(indexBytes, []byte("::"+string(jobId))...)

	err := s.store.IterateObjectsRaw(indexBytes, func(bytes []byte) error {
		state := &SyncJobState{}
		err := json.Unmarshal(bytes, state)
		if err != nil {
			return err
		}
		items = append(items, state)
		return nil
	})
	if err != nil {
		return err
	}
	for _, item := range items {
		err := s.store.DeleteObject(server.JOB_DATA_INDEX, item.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DataHubStoreSyncState) Reset(jobId scheduler.JobId, taskId string) error {
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	return s.store.DeleteObject(server.JOB_DATA_INDEX, id)
}
