// Copyright 2021 MIMIRO AS
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

package server

import (
	"context"
	"encoding/binary"
	"os"
	"os/exec"

	"github.com/bamzi/jobrunner"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type BackupManager struct {
	backupLocation string
	storeLocation  string
	schedule       string
	useRsync       bool
	lastID         uint64
	isRunning      bool
	store          *Store
	logger         *zap.SugaredLogger
}

func NewBackupManager(lc fx.Lifecycle, store *Store, env *conf.Env) (*BackupManager, error) {

	if env.BackupLocation == "" {
		return nil, nil
	}

	if env.BackupSchedule == "" {
		return nil, nil
	}

	backup := &BackupManager{}
	backup.backupLocation = env.BackupLocation
	backup.schedule = env.BackupSchedule
	backup.storeLocation = env.StoreLocation
	backup.useRsync = env.BackupRsync
	backup.store = store
	backup.logger = env.Logger.Named("backup")

	// load last id
	lastID, err := backup.LoadLastId()
	if err != nil {
		return nil, err
	}
	backup.lastID = lastID

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			backup.logger.Info("init backup via hook")
			schedule, err := cron.ParseStandard(backup.schedule)
			if err != nil {
				backup.logger.Error("bad cron schedule for backup job")
				return err
			}

			// pass in this backupManager struct. function Run will be called by the scheduler
			jobrunner.MainCron.Schedule(schedule, jobrunner.New(backup))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return nil
		},
	})

	return backup, nil
}

// This is the function called by the cron job scheduler
func (backupManager *BackupManager) Run() {
	if backupManager.isRunning {
		return
	}

	backupManager.isRunning = true

	if backupManager.useRsync {
		err := backupManager.DoRsyncBackup()
		if err != nil {
			backupManager.logger.Error("Error with rsync backup: " + err.Error())
		}
	} else {
		err := backupManager.DoNativeBackup()
		if err != nil {
			backupManager.logger.Error("Error with native backup: " + err.Error())
			panic("Error with native backup")
		}
	}

	backupManager.isRunning = false
}

func (backupManager *BackupManager) DoRsyncBackup() error {
	err := os.MkdirAll(backupManager.backupLocation, 0700)
	if err != nil {
		return err
	}

	cmd := exec.Command("rsync", "-avz", "--delete", backupManager.storeLocation, backupManager.backupLocation)
	err = cmd.Run()
	return err
}

func (backupManager *BackupManager) DoNativeBackup() error {
	err := os.MkdirAll(backupManager.backupLocation, 0700)
	if err != nil {
		return err
	}
	backupFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backup.kv"
	var file *os.File
	if backupManager.fileExists(backupFilename) {
		file, _ = os.Open(backupFilename)
	} else {
		file, _ = os.Create(backupFilename)
	}
	defer file.Close()
	since, _ := backupManager.store.database.Backup(file, backupManager.lastID)
	backupManager.lastID = since

	// store last id
	return backupManager.StoreLastId()
}

func (backupManager *BackupManager) StoreLastId() error {
	lastIDFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backup.lastseen"
	file, _ := os.Create(lastIDFilename)
	defer file.Close()

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, backupManager.lastID)
	_, err := file.Write(data)
	return err
}

func (backupManager *BackupManager) LoadLastId() (uint64, error) {
	lastIDFilename := backupManager.backupLocation + string(os.PathSeparator) + "datahub-backupManager.lastseen"
	file, err := os.Open(lastIDFilename)
	if err != nil {
		return 0, nil
	}
	defer file.Close()

	data := make([]byte, 8)
	_, err = file.Read(data)
	lastID := binary.LittleEndian.Uint64(data)
	return lastID, nil
}

func (backupManager *BackupManager) fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
