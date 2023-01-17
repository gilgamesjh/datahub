package jobs

import (
	"context"
	"errors"
	"fmt"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/zap"
	"time"
)

var (
	ErrJobNotFound = errors.New("job configuration not found")
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
		state:  p.SyncState,
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
	state  SyncState
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
	state, err := api.runner.RunningState(jobConfiguration.Id)
	if err != nil {
		return err
	}
	if state == nil || (state != nil && len(jobConfiguration.Tasks) != len(state.Tasks)) {
		if state != nil && len(jobConfiguration.Tasks) != len(state.Tasks) { // job tasks has been changed, need to reload
			err := api.runner.RemoveJob(jobConfiguration.Id, false)
			if err != nil {
				return err
			}
		}
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
			pipeline, err := api.builder.buildV2(v2job.Id, lookup[t.Id])
			if err != nil {
				return err
			}
			t.Fn = func(ctx context.Context, task *scheduler.JobTask) error {
				return pipeline.sync(ctx)
			}
		}

		_, err = api.runner.Schedule(jobConfiguration.Schedule, false, v2job)
		if err != nil {
			return err
		}
		//
	} else {
		return api.runner.ForceReload(jobConfiguration.Id)
	}
	return nil
}

func (api *Api) GetJob(jobId scheduler.JobId) (*scheduler.JobConfiguration, error) {
	return api.store.GetConfiguration(jobId)
}

func (api *Api) DeleteJob(jobId scheduler.JobId) error {
	return api.runner.RemoveJob(jobId, true)
}

func (api *Api) ListJobs() ([]*scheduler.JobConfiguration, error) {
	return api.store.ListConfigurations()
}

// GetScheduleEntries returns a cron list of all scheduled entries currently scheduled.
// Paused jobs are not part of this list
func (api *Api) GetScheduleEntries() (*ScheduleEntries, error) {
	se := make([]ScheduleEntry, 0)

	schedules, err := api.runner.Schedules()
	if err != nil {
		return nil, err
	}

	for _, e := range schedules {
		se = append(se, ScheduleEntry{
			Id:       int(e.EntryID),
			JobId:    string(e.Job.Id),
			JobTitle: e.Job.Title,
			Next:     e.Next,
			Prev:     e.Prev,
			Enabled:  e.Job.Enabled,
		})
	}

	entries := &ScheduleEntries{
		Entries: se,
	}

	return entries, nil
}

type JobStatus struct {
	JobId    string    `json:"jobId"`
	JobTitle string    `json:"jobTitle"`
	Started  time.Time `json:"started"`
	Status   string    `json:"status"`
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

func (api *Api) ListJobStatus() ([]scheduler.JobEntry, error) {
	return api.runner.JobEntries()
}

// GetRunningJob gets the status for a single running job. This can be used
// to see if a job is still running, and is currently used by the cli to follow
// a job run operation.
func (o *operations) GetRunningJob(jobId scheduler.JobId) (*scheduler.JobEntry, error) {
	return o.runner.RunningState(jobId)
}

func (api *Api) GetJobHistory(jobId scheduler.JobId, limit int) ([]*scheduler.JobHistory, error) {
	return api.store.GetJobHistory(jobId, limit)
}

// ListJobHistory returns a list of history for all jobs that have ever been run on the server.
func (api *Api) ListJobHistory() []*jobResult {
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

func (o *operations) Enable(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("missing config with id %s", jobId)
	}
	config.Enabled = true

	err = o.store.SaveConfiguration(jobId, config)
	if err != nil {
		return err
	}
	return o.runner.ForceReload(jobId)
}

func (o *operations) Disable(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("missing config with id %s", jobId)
	}
	config.Enabled = false

	err = o.store.SaveConfiguration(jobId, config)
	if err != nil {
		return err
	}
	return o.runner.ForceReload(jobId)
}

func (o *operations) Run(jobId scheduler.JobId) error {
	config, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return nil
	}
	if config == nil {
		return fmt.Errorf("%w with id %s", ErrJobNotFound, jobId)
	}

	return o.runner.RunJob(context.Background(), jobId)
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
	if jobConfiguration == nil {
		return fmt.Errorf("%w with id '%s'", ErrJobNotFound, jobId)
	}
	o.logger.Infof("Resetting since token for job with id '%s' (%s)", jobId, jobConfiguration.Title)

	for _, task := range jobConfiguration.Tasks {
		err := o.store.DeleteTask(jobId, task.Id) // we must also delete the current task state, not just the since state
		if err != nil {
			return err
		}
		state, err := o.state.Get(jobId, task.Id)
		if err != nil {
			return err
		}
		state.ContinuationToken = since
		err = o.state.Update(jobId, task.Id, state)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *operations) ResetTask(jobId scheduler.JobId, taskId, since string) error {
	jobConfiguration, err := o.store.GetConfiguration(jobId)
	if err != nil {
		return err
	}
	if jobConfiguration == nil {
		return fmt.Errorf("%w with id '%s'", ErrJobNotFound, jobId)
	}
	o.logger.Infof("Resetting since token for task with id '%s' (%s)", taskId, jobConfiguration.Title)
	err = o.store.DeleteTask(jobId, taskId) // we must also delete the current task state, not just the since state
	if err != nil {
		return err
	}

	state, err := o.state.Get(jobId, taskId)
	if err != nil {
		return err
	}
	state.ContinuationToken = since
	return o.state.Update(jobId, taskId, state)
}
