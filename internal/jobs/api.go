package jobs

import (
	"context"
	"fmt"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/zap"
	"time"
)

type Api struct {
	store      scheduler.Store
	runner     *scheduler.JobRunner
	builder    *pipelineBuilder
	Operations *operations
}

func NewApi(env *conf.Env, p SchedulerParams, runnerV2 *scheduler.JobRunner) *Api {

	ops := &operations{
		store:  p.JobStore,
		runner: runnerV2,
		ds:     p.Store,
		logger: env.Logger.Named("jobs"),
	}
	return &Api{
		store:      p.JobStore,
		runner:     runnerV2,
		builder:    newPipelineBuilder(env, p),
		Operations: ops,
	}
}

type operations struct {
	store  scheduler.Store
	ds     *server.Store
	runner *scheduler.JobRunner
	logger *zap.SugaredLogger
}

func (api *Api) UpdateJob(jobConfiguration *scheduler.JobConfiguration) error {
	// update the store first
	err := api.store.SaveConfiguration(jobConfiguration.Id, jobConfiguration)
	if err != nil {
		return err
	}

	// if state is missing, add to scheduler
	state := api.runner.RunningState(jobConfiguration.Id)
	if state == nil {
		jobConfiguration.DefaultFunc = func(ctx context.Context, task *scheduler.JobTask) error {
			return nil
		}

		v2job, err := jobConfiguration.ToJob(true)
		if err != nil {
			return err
		}

		lookup := make(map[string]*scheduler.TaskConfiguration)
		for _, t := range jobConfiguration.Tasks {
			lookup[t.Id] = t
		}

		for _, t := range v2job.Tasks {
			pipeline, err := api.builder.buildV2(lookup[t.Id])
			if err != nil {
				return err
			}
			t.Fn = func(ctx context.Context, task *scheduler.JobTask) error {
				return pipeline.sync(t, ctx)
			}
		}

		_, err = api.runner.Schedule(jobConfiguration.Schedule, false, v2job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (api *Api) GetJob(jobId scheduler.JobId) (*scheduler.JobConfiguration, error) {
	return api.store.GetConfiguration(jobId)
}

func (api *Api) DeleteJob(jobId scheduler.JobId) error {
	return api.runner.RemoveJob(jobId)
}

func (api *Api) ListJobs() ([]*scheduler.JobConfiguration, error) {
	return api.store.ListConfigurations()
}

// GetScheduleEntries returns a cron list of all scheduled entries currently scheduled.
// Paused jobs are not part of this list
func (api *Api) GetScheduleEntries() ScheduleEntries {
	se := make([]ScheduleEntry, 0)

	for _, e := range api.runner.Schedules() {
		se = append(se, ScheduleEntry{
			Id:       int(e.EntryID),
			JobId:    string(e.Job.Id),
			JobTitle: e.Job.Title,
			Next:     e.Next,
			Prev:     e.Prev,
			Enabled:  !e.Job.Paused,
		})
	}

	entries := ScheduleEntries{
		Entries: se,
	}

	return entries
}

type JobStatus struct {
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Started  time.Time `json:"started"`
}

// GetRunningJobs gets the status for all running jobs. It can be used to see
// what the job system is currently doing.
func (api *Api) GetRunningJobs() []JobStatus {
	return nil
	/*runningJobs := api.runner.Schedules()
	jobs := make([]JobStatus, 0)

	for k, v := range runningJobs {

		jobs = append(jobs, JobStatus{
			JobId:    k,
			JobTitle: v.title,
			Started:  v.started,
		})
	}
	return jobs*/
}

// GetRunningJob gets the status for a single running job. This can be used
// to see if a job is still running, and is currently used by the cli to follow
// a job run operation.
func (o *operations) GetRunningJob(jobId scheduler.JobId) *JobStatus {
	runningJob := o.runner.RunningState(jobId)
	if runningJob == nil {
		return nil
	}
	if runningJob.State != scheduler.WorkerStateRunning {
		return nil
	}
	return &JobStatus{
		JobId:    string(jobId),
		JobTitle: runningJob.JobTitle,
		Started:  runningJob.Started,
	}

}

// GetJobHistory returns a list of history for all jobs that have ever been run on the server. It could be that in the
// future this will only return the history of the currently registered jobs.
// Each job stores its Start and End time, together with the last error if any.
func (api *Api) GetJobHistory() []*jobResult {
	items, _ := api.store.ListJobHistory(-1)
	results := make([]*jobResult, 0)

	for _, item := range items {
		results = append(results, &jobResult{
			Id:        string(item.JobId),
			Title:     item.Title,
			Start:     item.Start,
			End:       item.End,
			LastError: item.LastError,
		})
	}
	return results
}

func (o *operations) Pause(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("missing config with id %s", jobId)
	}
	config.Paused = true
	return o.store.SaveConfiguration(jobId, config)
}

func (o *operations) Resume(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("missing config with id %s", jobId)
	}
	config.Paused = false
	return o.store.SaveConfiguration(jobId, config)
}

func (o *operations) Run(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("missing config with id %s", jobId)
	}
	job, err := config.ToJob(false)
	if err != nil {
		return err
	}

	return o.runner.RunJob(context.Background(), job)
}

func (o *operations) Terminate(jobId scheduler.JobId) error {
	o.runner.CancelJob(jobId)
	return nil
}

// Reset will reset the job since token. This allows the job to be rerun from the beginning
func (o *operations) Reset(jobId scheduler.JobId, since string) error {
	jobConfiguration, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return err
	}
	o.logger.Infof("Resetting since token for job with id '%s' (%s)", jobId, jobConfiguration.Title)

	syncJobState := &SyncJobState{}
	err = o.ds.GetObject(server.JOB_DATA_INDEX, string(jobId), syncJobState)
	if err != nil {
		return err
	}

	if syncJobState.ID == "" {
		return nil
	}

	syncJobState.ContinuationToken = since
	err = o.ds.StoreObject(server.JOB_DATA_INDEX, string(jobId), syncJobState)
	if err != nil {
		return err
	}

	return nil
}
