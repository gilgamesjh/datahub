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
	"errors"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/mimiro-io/datahub/internal/jobs"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type jobOperationHandler struct {
	jobScheduler *jobs.Scheduler
	api          *jobs.Api
}

func NewJobOperationHandler(lc fx.Lifecycle, e *echo.Echo, logger *zap.SugaredLogger, mw *Middleware, api *jobs.Api, js *jobs.Scheduler) {
	log := logger.Named("web")
	handler := &jobOperationHandler{
		jobScheduler: js,
		api:          api,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.PUT("/job/:jobid/pause", handler.jobsDisable, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/resume", handler.jobsEnable, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/disable", handler.jobsDisable, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/enable", handler.jobsEnable, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/kill", handler.jobsKill, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/run", handler.jobsRun, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/reset", handler.jobsReset, mw.authorizer(log, datahubWrite))
			e.PUT("/job/:jobid/task/:taskId/reset", handler.taskReset, mw.authorizer(log, datahubWrite))
			e.GET("/job/:jobid/status", handler.jobsGetStatus, mw.authorizer(log, datahubRead)) // is it running

			return nil
		},
	})
}

func (handler *jobOperationHandler) jobsEnable(c echo.Context) error {
	jobId := c.Param("jobid")
	err := handler.api.Operations.Enable(scheduler.JobId(jobId))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "could not enable job")
	}
	return c.JSON(http.StatusOK, &JobResponse{JobId: jobId})
}

func (handler *jobOperationHandler) jobsDisable(c echo.Context) error {
	jobId := c.Param("jobid")
	err := handler.api.Operations.Disable(scheduler.JobId(jobId))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "could not disable job")
	}
	return c.JSON(http.StatusOK, &JobResponse{JobId: jobId})
}

func (handler *jobOperationHandler) jobsKill(c echo.Context) error {
	jobId := c.Param("jobid")
	err := handler.api.Operations.Terminate(scheduler.JobId(jobId))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, &JobResponse{JobId: jobId})
}

func (handler *jobOperationHandler) jobsRun(c echo.Context) error {
	jobId := c.Param("jobid")
	err := handler.api.Operations.Run(scheduler.JobId(jobId))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "could not un-pause job")
	}

	return c.JSON(http.StatusOK, &JobResponse{JobId: "0"})
}

func (handler *jobOperationHandler) taskReset(c echo.Context) error {
	jobId := c.Param("jobid")
	taskId := c.Param("taskId")
	since := c.QueryParam("since")

	err := handler.api.Operations.ResetTask(scheduler.JobId(jobId), taskId, since)
	if err != nil {
		if errors.Is(err, jobs.ErrJobNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, &JobResponse{JobId: jobId})
}

func (handler *jobOperationHandler) jobsReset(c echo.Context) error {
	jobId := c.Param("jobid")
	since := c.QueryParam("since")

	err := handler.api.Operations.Reset(scheduler.JobId(jobId), since)
	if err != nil {
		if errors.Is(err, jobs.ErrJobNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, &JobResponse{JobId: jobId})
}

func (handler *jobOperationHandler) jobsGetStatus(c echo.Context) error {
	jobId := c.Param("jobid")

	status, err := handler.api.Operations.GetRunningJob(scheduler.JobId(jobId))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if status == nil {
		return c.JSON(http.StatusOK, []*scheduler.JobEntry{})
	}

	return c.JSON(http.StatusOK, []*scheduler.JobEntry{status}) // converted to a list
}
