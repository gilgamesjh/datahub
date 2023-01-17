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

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/lucasepe/codename"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"net/http"
	"strconv"
	"time"

	"github.com/mimiro-io/datahub/internal/jobs/source"

	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// The Scheduler deals with reading and writing jobs and making sure they get added to the
// job Runner. It also deals with translating between the external JobConfiguration and the
// internal job format.
type Scheduler struct {
	Logger         *zap.SugaredLogger
	Store          *server.Store
	DatasetManager *server.DsManager

	runnerV2 *scheduler.JobRunner
	storeV2  scheduler.Store
	api      *Api
}

const TriggerTypeCron = "cron"
const TriggerTypeOnChange = "onchange"
const JobTypeFull = "fullsync"
const JobTypeIncremental = "incremental"

var TriggerTypes = map[string]bool{TriggerTypeOnChange: true, TriggerTypeCron: true}
var JobTypes = map[string]bool{JobTypeFull: true, JobTypeIncremental: true}

type JobTrigger struct {
	TriggerType      string `json:"triggerType"`
	JobType          string `json:"jobType"`
	Schedule         string `json:"schedule"`
	MonitoredDataset string `json:"monitoredDataset"`
}

// JobConfiguration is the external interfacing object to configure a job. It is also the one that gets persisted
// in the store.
type JobConfiguration struct {
	Id          string                 `json:"id"`
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Tags        []string               `json:"tags"`
	Source      map[string]interface{} `json:"source"`
	Sink        map[string]interface{} `json:"sink"`
	Transform   map[string]interface{} `json:"transform"`
	Triggers    []JobTrigger           `json:"triggers"`
	Paused      bool                   `json:"paused"`
	BatchSize   int                    `json:"batchSize"`
}

type ScheduleEntries struct {
	Entries []ScheduleEntry `json:"entries"`
}

type ScheduleEntry struct {
	Id       int       `json:"id"`
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Next     time.Time `json:"next"`
	Prev     time.Time `json:"prev"`
	Enabled  bool      `json:"enabled"`
}

type SchedulerParams struct {
	fx.In

	Store          *server.Store
	Dsm            *server.DsManager
	TokenProviders *security.TokenProviders
	JobStore       scheduler.Store
	SyncState      SyncState
	Statsd         statsd.ClientInterface
	EventBus       server.EventBus
}

func NewV2Scheduler(env *conf.Env, p SchedulerParams) *scheduler.JobRunner {
	return scheduler.NewJobRunner(
		scheduler.NewJobScheduler(env.Logger, "jobs", p.JobStore, int32(env.RunnerConfig.JobQueueConcurrency)),
		scheduler.NewTaskScheduler(env.Logger, "tasks", p.JobStore, int32(env.RunnerConfig.TaskQueueConcurrency)),
		p.Statsd)
}

// NewScheduler returns a new Scheduler. When started, it will load all existing JobConfiguration's from the store,
// and schedule this with the runner.
func NewScheduler(lc fx.Lifecycle, env *conf.Env, p SchedulerParams, runnerV2 *scheduler.JobRunner, api *Api) *Scheduler {

	s := &Scheduler{
		Logger:         env.Logger.Named("scheduler"),
		Store:          p.Store,
		DatasetManager: p.Dsm,
		storeV2:        p.JobStore,
		api:            api,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			s.Logger.Infof("Starting the JobScheduler")

			s.runnerV2 = runnerV2

			loadedJobs := make(map[string]struct{})
			configs, _ := p.JobStore.ListConfigurations()
			for _, j := range configs {
				err := api.UpdateJob(j)
				if err != nil {
					s.Logger.Warnf("Error loading job with id %s (%s), err: %v", j.Id, j.Title, err)
				}
				loadedJobs[string(j.Id)] = struct{}{}
			}

			for _, j := range s.loadConfigurations() {
				if _, ok := loadedJobs[j.Id]; !ok { // only upgrade job if not loaded already
					err := s.AddJob(j)
					if err != nil {
						s.Logger.Warnf("Error loading job with id %s (%s), err: %v", j.Id, j.Title, err)
					}
				}
			}

			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.Logger.Infof("Stopping job runner")
			// TODO: Add stop method to new runner
			return nil
		},
	})

	return s
}

func (s *Scheduler) toV2(jobConfig *JobConfiguration, jobs []*job) error {
	var runnableJob *job
	for _, j := range jobs {
		if !j.isEvent {
			runnableJob = j
			break
		}
	}
	if runnableJob == nil {
		return nil
	}

	configV2 := &scheduler.JobConfiguration{
		Id:              scheduler.JobId(jobConfig.Id),
		Title:           jobConfig.Title,
		Version:         scheduler.JobConfigurationVersion1,
		Description:     jobConfig.Description,
		Tags:            jobConfig.Tags,
		Enabled:         !jobConfig.Paused,
		BatchSize:       jobConfig.BatchSize,
		ResumeOnRestart: true, // always resume failed tasks as this is the expected behaviour for now
		OnError:         []string{"SuccessReport"},
		OnSuccess:       []string{"SuccessReport"},
		Schedule:        runnableJob.schedule,
		Tasks: []*scheduler.TaskConfiguration{
			{
				Id:          jobConfig.Id,
				Name:        jobConfig.Title + " Task 1",
				Description: jobConfig.Description,
				BatchSize:   jobConfig.BatchSize,
				Source:      jobConfig.Source,
				Sink:        jobConfig.Sink,
				Transform:   jobConfig.Transform,
				Type:        jobConfig.Triggers[0].JobType,
				DependsOn:   nil,
			},
		},
	}

	err := s.api.UpdateJob(configV2)
	if err != nil {
		return err
	}

	/*_ = s.storeV2.SaveConfiguration(configV2.Id, configV2)
	v2job, err := configV2.ToJob(true)
	v2job.Tasks[0].Fn = func(ctx context.Context, task *scheduler.JobTask) error {
		return runnableJob.pipeline.sync(v2job.Tasks[0], ctx)
	}
	if err != nil {
		return nil, err
	}*/

	return nil
}

// AddJob takes an incoming JobConfiguration and stores it in the store
// Once it has stored it, it will transform it to a Pipeline and add it to the scheduler
// It is important that jobs are valid, so care is taken to validate the JobConfiguration before
// it can be scheduled.
func (s *Scheduler) AddJob(jobConfig *JobConfiguration) error {
	// TODO: redo this as a pure upgrade function
	err := s.verify(jobConfig)
	if err != nil {
		return err
	}
	return nil

	// this also verifies that it can be parsed, so do this first
	/*triggeredJobs, err := s.toTriggeredJobs(jobConfig)
	if err != nil {
		return err
	}

	err = s.Store.StoreObject(server.JOB_CONFIGS_INDEX, jobConfig.Id, jobConfig) // store it for the future
	if err != nil {
		return err
	}

	return s.toV2(jobConfig, triggeredJobs)
	/*if err != nil {
		return err
	}

	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		// make sure we clear up before adding
		clearCrontab(s.Runner.scheduledJobs, jobConfig.Id)
		s.Runner.eventBus.UnsubscribeToDataset(jobConfig.Id)
		for _, job := range triggeredJobs {
			if !jobConfig.Paused { // only add the job if it is not paused
				if job.isEvent {
					err := s.Runner.addJob(job)
					if err != nil {
						return err
					}
				}

			} else {
				s.Logger.Infof("Job '%s' is currently paused, it will not be started automatically", jobConfig.Id)
			}
		}
		return nil
	})

	return g.Wait()*/
}

// extractJobs extracts the jobs configured in the JobConfiguration and extracts them as a
// list of jobs to be scheduled, depending on the type
func (s *Scheduler) toTriggeredJobs(jobConfig *JobConfiguration) ([]*job, error) {
	var result []*job
	for _, t := range jobConfig.Triggers {
		pipeline, err := s.toPipeline(jobConfig, t.JobType) // this also verifies that it can be parsed, so do this first
		if err != nil {
			return nil, err
		}
		switch t.TriggerType {
		case TriggerTypeOnChange:
			result = append(result, &job{
				id:       jobConfig.Id,
				title:    jobConfig.Title,
				pipeline: pipeline,
				topic:    t.MonitoredDataset,
				isEvent:  true,
			})
		case TriggerTypeCron:
			result = append(result, &job{
				id:       jobConfig.Id,
				title:    jobConfig.Title,
				pipeline: pipeline,
				schedule: t.Schedule,
			})
		default:
			return nil, errors.New(fmt.Sprintf("could not map trigger configuration to job: %v", t))
		}
	}
	return result, nil
}

// LoadJob will attempt to load a JobConfiguration based on a jobId. Because of the GetObject method currently
// works, it will not return nil when not found, but an empty jobConfig object.
func (s *Scheduler) LoadJob(jobId string) (*JobConfiguration, error) {
	jobConfig := &JobConfiguration{}
	err := s.Store.GetObject(server.JOB_CONFIGS_INDEX, jobId, jobConfig)
	if err != nil {
		return nil, err
	}
	return jobConfig, nil
}

// ListJobs returns a list of all stored configurations
func (s *Scheduler) ListJobs() []*JobConfiguration {
	return s.loadConfigurations()
}

// Parse is a convenience method to parse raw config json into a JobConfiguration
func (s *Scheduler) Parse(rawJson []byte) (*JobConfiguration, error) {
	config := &JobConfiguration{}
	err := json.Unmarshal(rawJson, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// loadConfigurations is the internal method to load JobConfiguration's from the store, it is currently different
// from the needs of the ListJobs call, but I wanted to separate them.
func (s *Scheduler) loadConfigurations() []*JobConfiguration {
	jobConfigs := []*JobConfiguration{}

	_ = s.Store.IterateObjectsRaw(server.JOB_CONFIGS_INDEX_BYTES, func(jsonData []byte) error {
		jobConfig := &JobConfiguration{}
		err := json.Unmarshal(jsonData, jobConfig)

		if err != nil {
			s.Logger.Warnf(" > Error parsing job from store - aborting start: %s", err)
			return err
		}
		jobConfigs = append(jobConfigs, jobConfig)

		return nil
	})
	return jobConfigs
}

// verify makes sure a JobConfiguration is valid
func (s *Scheduler) verify(jobConfiguration *JobConfiguration) error {
	if len(jobConfiguration.Id) <= 0 {
		return errors.New("job configuration needs an id")
	}
	if len(jobConfiguration.Title) <= 0 {
		rng, _ := codename.DefaultRNG()
		jobConfiguration.Title = codename.Generate(rng, 0)
		//return errors.New("job configuration needs a title")
	}
	for _, config := range s.ListJobs() {
		if config.Title == jobConfiguration.Title {
			if config.Id != jobConfiguration.Id {
				return errors.New("job configuration title must be unique")
			}
		}
	}
	// we need to have at least 1 sink & 1 source
	if len(jobConfiguration.Source) <= 0 {
		return errors.New("you must configure a source")
	}
	if len(jobConfiguration.Sink) <= 0 {
		return errors.New("you must configure a sink")
	}
	if len(jobConfiguration.Triggers) <= 0 {
		return errors.New("Job Configuration needs at least 1 trigger")
	}
	for _, trigger := range jobConfiguration.Triggers {
		if _, ok := TriggerTypes[trigger.TriggerType]; !ok {
			return errors.New("need to set 'triggerType'. must be one of: cron, onchange")
		}
		if _, ok := JobTypes[trigger.JobType]; !ok {
			return errors.New("need to set 'jobType'. must be one of: fullsync, incremental")
		}
		if trigger.TriggerType == TriggerTypeOnChange {
			// if an event handler is given, that is ok in this context, so we just pass it on
			if trigger.MonitoredDataset != "" {
				return nil
			}
			return errors.New("trigger type 'onchange' requires that 'MonitoredDataset' parameter also is set")
		}

		_, err := cron.ParseStandard(trigger.Schedule)
		if err != nil {
			return errors.New("trigger type " + trigger.TriggerType + " requires a valid 'Schedule' expression. But: " + err.Error())
		}
	}
	return nil
}

// toPipeline converts the json in the JobConfiguration to concrete types.
// A Pipeline is basically a Source -> Transform -> Sink
func (s *Scheduler) toPipeline(jobConfig *JobConfiguration, jobType string) (Pipeline, error) {
	sink, err := s.parseSink(jobConfig)
	if err != nil {
		return nil, err
	}
	source, err := s.parseSource(jobConfig)
	if err != nil {
		return nil, err
	}

	transform, err := s.parseTransform(jobConfig)
	if err != nil {
		return nil, err
	}

	batchSize := jobConfig.BatchSize
	if batchSize < 1 {
		batchSize = defaultBatchSize // this is the default batch size
	}

	pipeline := PipelineSpec{
		source:    source,
		sink:      sink,
		transform: transform,
		batchSize: batchSize,
	}

	if jobType == JobTypeFull {
		return &FullSyncPipeline{pipeline}, nil
	} else {
		return &IncrementalPipeline{pipeline}, nil
	}
}

func (s *Scheduler) parseSource(jobConfig *JobConfiguration) (source.Source, error) {
	sourceConfig := jobConfig.Source
	if sourceConfig != nil {
		sourceTypeName := sourceConfig["Type"]
		if sourceTypeName != nil {
			if sourceTypeName == "HttpDatasetSource" {
				src := &source.HttpDatasetSource{}
				src.Store = s.Store
				endpoint, ok := sourceConfig["Url"]
				if ok && endpoint != "" {
					src.Endpoint = endpoint.(string)
				}
				tokenProviderRaw, ok := sourceConfig["TokenProvider"]
				if ok {
					tokenProviderName := tokenProviderRaw.(string)
					// security
					if tokenProviderName != "" {
						// attempt to parse the token provider
						//if provider, ok := s.Runner.tokenProviders.Get(strings.ToLower(tokenProviderName)); ok {
						//	src.TokenProvider = provider
						//}
					}
				}
				return src, nil
			} else if sourceTypeName == "DatasetSource" {
				var err error
				src := &source.DatasetSource{}
				src.Store = s.Store
				src.DatasetManager = s.DatasetManager
				src.DatasetName = (sourceConfig["Name"]).(string)
				src.AuthorizeProxyRequest = func(authProviderName string) func(req *http.Request) {
					/*if s.Runner.tokenProviders != nil {
						if provider, ok := s.Runner.tokenProviders.Get(strings.ToLower(authProviderName)); ok {
							return provider.Authorize
						}
					}*/
					// if no authProvider is found, fall back to no auth for backend requests
					return func(req *http.Request) {
						//noop
					}
				}
				if sourceConfig["LatestOnly"] != nil {
					i := sourceConfig["LatestOnly"]
					if boolVal, ok := i.(bool); ok {
						src.LatestOnly = boolVal
					} else {
						src.LatestOnly, err = strconv.ParseBool(i.(string))
					}
				}
				if err != nil {
					return nil, err
				}
				return src, nil
			} else if sourceTypeName == "MultiSource" {
				src := &source.MultiSource{}
				src.Store = s.Store
				src.DatasetManager = s.DatasetManager
				src.DatasetName = (sourceConfig["Name"]).(string)
				err := src.ParseDependencies(sourceConfig["Dependencies"])
				if err != nil {
					return nil, err
				}
				if sourceConfig["LatestOnly"] != nil {
					i := sourceConfig["LatestOnly"]
					if boolVal, ok := i.(bool); ok {
						src.LatestOnly = boolVal
					} else {
						src.LatestOnly, err = strconv.ParseBool(i.(string))
					}
				}
				if err != nil {
					return nil, err
				}
				return src, nil
			} else if sourceTypeName == "UnionDatasetSource" {
				src := &source.UnionDatasetSource{}
				datasets, ok := sourceConfig["DatasetSources"].([]interface{})
				if ok {
					for _, dsSrcConfig := range datasets {
						if dsSrcConfigMap, ok2 := dsSrcConfig.(map[string]interface{}); ok2 {

							dsSrcConfigMap["Type"] = "DatasetSource"
							parseSource, err := s.parseSource(&JobConfiguration{Source: dsSrcConfigMap})
							if err != nil {
								return nil, err
							}
							src.DatasetSources = append(src.DatasetSources, parseSource.(*source.DatasetSource))
						} else {
							return nil, fmt.Errorf("could not parse dataset item in UnionDatasetSource %v: %v", jobConfig.Id, dsSrcConfig)
						}
					}
				} else {
					return nil, fmt.Errorf("could not parse UnionDatasetSource: %v", sourceConfig)
				}
				return src, nil
			} else if sourceTypeName == "SampleSource" {
				src := &source.SampleSource{}
				src.Store = s.Store
				numEntities := sourceConfig["NumberOfEntities"]
				if numEntities != nil {
					src.NumberOfEntities = int(numEntities.(float64))
				}
				return src, nil
			} else if sourceTypeName == "SlowSource" {
				src := &source.SlowSource{}
				src.Sleep = sourceConfig["Sleep"].(string)
				batch := sourceConfig["BatchSize"]
				if batch != nil {
					src.BatchSize = int(batch.(float64))
				}
				return src, nil
			} else {
				return nil, errors.New("unknown source type: " + sourceTypeName.(string))
			}
		}
		return nil, errors.New("missing source type")
	}
	return nil, errors.New("missing source config")

}

func (s *Scheduler) resolveJobTitle(jobId string) string {
	jobConfig, err := s.LoadJob(jobId)
	if err != nil {
		s.Logger.Warnf("Failed to resolve title for job id '%s'", jobId)
		return ""
	}
	return jobConfig.Title
}
