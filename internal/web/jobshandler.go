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

package web

import (
	"context"
	"encoding/json"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/jobs"
	"github.com/mimiro-io/datahub/internal/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// This is used to give the web handler a type, instead of just a map
// The reason for this, is that it makes it less random how the json
// gets transformed. This should probably be done with Source and Sink as well

type JobResponse struct {
	JobId string `json:"jobId"`
}

type jobsHandler struct {
	api          *jobs.Api
	jobScheduler *jobs.Scheduler
}

func NewJobsHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, api *jobs.Api, jobScheduler *jobs.Scheduler) {
	log := logger.Named("web")
	handler := &jobsHandler{
		api: api,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// jobs
			e.GET("/jobs", handler.jobsList, mw.authorizer(log, datahubRead)) // list of all defined jobs

			// internal usage
			e.GET("/jobs/_/schedules", handler.jobsListSchedules, mw.authorizer(log, datahubRead))
			e.GET("/jobs/_/status", handler.jobsListStatus, mw.authorizer(log, datahubRead))
			e.GET("/jobs/_/history", handler.jobsListHistory, mw.authorizer(log, datahubRead))
			e.GET("/jobs/_/history/:jobid", handler.jobsGetHistory, mw.authorizer(log, datahubRead))
			e.GET("/jobs/:jobid", handler.jobsGetDefinition, mw.authorizer(log, datahubRead)) // the json used to define it
			e.DELETE("/jobs/:jobid", handler.jobsDelete, mw.authorizer(log, datahubWrite))    // remove an existing job
			e.POST("/jobs", handler.jobsAdd, mw.authorizer(log, datahubWrite))

			return nil
		},
	})

}

func (handler *jobsHandler) jobsList(c echo.Context) error {
	jobsList, err := handler.api.ListJobs()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobsList)
}

func (handler *jobsHandler) jobsAdd(c echo.Context) error {
	// read json
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpBodyMissingErr(err).Error())
	}

	// we support 2 different versions going forward, so let's parse, and then look at what we have
	v2 := &scheduler.JobConfiguration{}
	err = json.Unmarshal(body, v2)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, server.HttpJobParsingErr(err).Error())
	}
	if v2.Version == "" || v2.Version == scheduler.JobConfigurationVersion1 {
		// this is a v1, lets parse it, and then convert it
		v1 := jobs.JobConfiguration{}
		err = json.Unmarshal(body, &v1)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, server.HttpJobParsingErr(err).Error())
		}

		taskType := jobs.JobTypeIncremental
		schedule := ""
		for _, t := range v1.Triggers {
			if t.JobType != "" {
				taskType = t.JobType
				schedule = t.Schedule
				break
			}
		}

		v2.Id = scheduler.JobId(v1.Id)
		v2.Title = v1.Title
		v2.Version = scheduler.JobConfigurationVersion1
		v2.Description = v1.Description
		v2.Tags = v1.Tags
		v2.Schedule = schedule
		v2.BatchSize = v1.BatchSize
		v2.OnSuccess = []string{"SuccessHandler"}
		v2.OnError = []string{"SuccessHandler"}
		v2.Tasks = []*scheduler.TaskConfiguration{
			{
				Id:          v1.Id,
				Name:        v1.Title,
				Description: v1.Description,
				BatchSize:   v1.BatchSize,
				Type:        taskType,
				Source:      v1.Source,
				Sink:        v1.Sink,
				Transform:   v1.Transform,
			},
		}
	}

	err = handler.api.UpdateJob(v2)
	//config, err := handler.jobScheduler.Parse(body)

	//err = handler.jobScheduler.AddJob(config)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, &JobResponse{JobId: string(v2.Id)})
}

func (handler *jobsHandler) jobsGetDefinition(c echo.Context) error {
	jobId := c.Param("jobid")

	res, err := handler.api.GetJob(scheduler.JobId(jobId))
	if err != nil || res.Id == "" {
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, res)

}

func (handler *jobsHandler) jobsListSchedules(c echo.Context) error {
	schedules, err := handler.api.GetScheduleEntries()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, schedules)
}

func (handler *jobsHandler) jobsListStatus(c echo.Context) error {
	items, err := handler.api.ListJobStatus()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, items)
}

func (handler *jobsHandler) jobsListHistory(c echo.Context) error {
	return c.JSON(http.StatusOK, handler.api.ListJobHistory())
}

func (handler *jobsHandler) jobsGetHistory(c echo.Context) error {
	jobId := c.Param("jobid")
	limit := 1
	aLimit := c.QueryParam("limit")
	if aLimit != "" {
		v, err := strconv.Atoi(aLimit)
		if err == nil { // we just use default on error
			limit = v
		}
	}
	if history, err := handler.api.GetJobHistory(scheduler.JobId(jobId), limit); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	} else {
		return c.JSON(http.StatusOK, history)
	}

}

// jobsDelete will delete a job with the given jobid if it exists
// it should return 200 OK when successful, but 404 if the job id
// does not exists
func (handler *jobsHandler) jobsDelete(c echo.Context) error {
	jobId, _ := url.QueryUnescape(c.Param("jobid"))

	err := handler.api.DeleteJob(scheduler.JobId(jobId))
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}
	return c.NoContent(http.StatusOK)
}
