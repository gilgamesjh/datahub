package jobs

import (
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/jobs/source"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/datahub/internal/server"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"strings"
)

type pipelineBuilder struct {
	dsm            *server.DsManager
	store          *server.Store
	logger         *zap.SugaredLogger
	tokenProviders *security.TokenProviders
	eventBus       server.EventBus
	statsdClient   statsd.ClientInterface
	state          SyncState
}

func newPipelineBuilder(env *conf.Env, p SchedulerParams) *pipelineBuilder {
	return &pipelineBuilder{
		dsm:            p.Dsm,
		store:          p.Store,
		logger:         env.Logger,
		state:          p.SyncState,
		tokenProviders: p.TokenProviders,
		eventBus:       p.EventBus,
		statsdClient:   p.Statsd,
	}
}

func (b *pipelineBuilder) buildV2(jobId scheduler.JobId, task *scheduler.TaskConfiguration) (Pipeline, error) {
	sink, err := b.buildSink(task.Sink)
	if err != nil {
		return nil, err
	}

	src, err := b.buildSource(task.Source)
	if err != nil {
		return nil, err
	}

	t, err := b.buildTransform(task.Transform)
	if err != nil {
		return nil, err
	}

	pipeline := PipelineSpec{
		task: struct {
			jobId  scheduler.JobId
			taskId string
			name   string
		}{jobId: jobId, taskId: task.Id, name: task.Name},
		source:       src,
		sink:         sink,
		transform:    t,
		store:        b.store,
		state:        b.state,
		batchSize:    defaultBatchSize,
		statsdClient: b.statsdClient,
		eventBus:     b.eventBus,
	}
	if task.BatchSize != 0 {
		pipeline.batchSize = task.BatchSize
	}

	if strings.ToLower(task.Type) == JobTypeFull {
		return &FullSyncPipeline{pipeline}, nil
	} else {
		return &IncrementalPipeline{pipeline}, nil
	}

}

func (b *pipelineBuilder) buildSink(sinkConfig map[string]any) (Sink, error) {
	// do pass to lower case the keys
	for k, v := range sinkConfig {
		sinkConfig[strings.ToLower(k)] = v
	}

	if sinkConfig != nil {
		sinkTypeName := sinkConfig["type"]
		if sinkTypeName != nil {
			if sinkTypeName == "DatasetSink" {
				dsname := (sinkConfig["name"]).(string)
				dataset := b.dsm.GetDataset(dsname)
				if dataset != nil && dataset.IsProxy() {
					sink := &httpDatasetSink{}
					sink.Store = b.store
					sink.logger = b.logger.Named("sink")
					sink.Endpoint, _ = server.UrlJoin(dataset.ProxyConfig.RemoteUrl, "/entities")

					if dataset.ProxyConfig.AuthProviderName != "" {
						sink.TokenProvider = dataset.ProxyConfig.AuthProviderName
						sink.TokenProviders = b.tokenProviders
					}
					return sink, nil
				}
				sink := &datasetSink{}
				sink.DatasetName = dsname
				sink.Store = b.store
				sink.DatasetManager = b.dsm

				return sink, nil
			} else if sinkTypeName == "DevNullSink" {
				sink := &devNullSink{}
				return sink, nil
			} else if sinkTypeName == "ConsoleSink" {
				sink := &consoleSink{}
				v, ok := sinkConfig["prefix"]
				if ok {
					sink.Prefix = v.(string)
				}
				v, ok = sinkConfig["detailed"]
				if ok {
					sink.Detailed = v.(bool)
				}
				sink.logger = b.logger

				return sink, nil
			} else if sinkTypeName == "HttpDatasetSink" {
				sink := &httpDatasetSink{}
				sink.Store = b.store
				sink.TokenProviders = b.tokenProviders
				sink.logger = b.logger.Named("sink")

				endpoint, ok := sinkConfig["url"]
				if ok && endpoint != "" {
					sink.Endpoint = endpoint.(string)
				}
				tokenProvider, ok := sinkConfig["tokenprovider"]
				if ok {
					sink.TokenProvider = tokenProvider.(string)
				}
				sink.TokenProviders = b.tokenProviders
				return sink, nil
			} else {
				return nil, errors.New("unknown sink type: " + sinkTypeName.(string))
			}
		}
		return nil, errors.New("missing sink type")
	}
	return nil, errors.New("missing or wrong sink type")
}

func (b *pipelineBuilder) buildSource(sourceConfig map[string]any) (source.Source, error) {
	// do a pass and lowercase the keys
	for k, v := range sourceConfig {
		sourceConfig[strings.ToLower(k)] = v
	}

	if sourceConfig != nil {
		sourceTypeName := sourceConfig["type"]
		if sourceTypeName != nil {
			if sourceTypeName == "HttpDatasetSource" {
				src := &source.HttpDatasetSource{}
				src.Store = b.store
				src.Logger = b.logger.Named("HttpDatasetSource")
				endpoint, ok := sourceConfig["url"]
				if ok && endpoint != "" {
					src.Endpoint = endpoint.(string)
				}
				tokenProviderRaw, ok := sourceConfig["tokenprovider"]
				if ok {
					tokenProviderName := tokenProviderRaw.(string)
					// security
					if tokenProviderName != "" {
						// attempt to parse the token provider
						if provider, ok := b.tokenProviders.Get(strings.ToLower(tokenProviderName)); ok {
							src.TokenProvider = provider
						}
					}
				}

				return src, nil
			} else if sourceTypeName == "DatasetSource" {
				var err error
				src := &source.DatasetSource{}
				src.Store = b.store
				src.DatasetManager = b.dsm
				src.DatasetName = (sourceConfig["name"]).(string)
				src.AuthorizeProxyRequest = func(authProviderName string) func(req *http.Request) {
					if b.tokenProviders != nil {
						if provider, ok := b.tokenProviders.Get(strings.ToLower(authProviderName)); ok {
							return provider.Authorize
						}
					}
					// if no authProvider is found, fall back to no auth for backend requests
					return func(req *http.Request) {
						//noop
					}
				}
				if sourceConfig["latestonly"] != nil {
					i := sourceConfig["latestonly"]
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
				src.Store = b.store
				src.DatasetManager = b.dsm
				src.DatasetName = (sourceConfig["name"]).(string)
				err := src.ParseDependencies(sourceConfig["dependencies"])
				if err != nil {
					return nil, err
				}
				if sourceConfig["latestonly"] != nil {
					i := sourceConfig["latestonly"]
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
				datasets, ok := sourceConfig["datasetsources"].([]interface{})
				if ok {
					for _, dsSrcConfig := range datasets {
						if dsSrcConfigMap, ok2 := dsSrcConfig.(map[string]interface{}); ok2 {

							dsSrcConfigMap["Type"] = "DatasetSource"
							parseSource, err := b.buildSource(dsSrcConfigMap)
							if err != nil {
								return nil, err
							}
							src.DatasetSources = append(src.DatasetSources, parseSource.(*source.DatasetSource))
						} else {
							return nil, fmt.Errorf("could not parse dataset item in UnionDatasetSource %v", dsSrcConfig)
						}
					}
				} else {
					return nil, fmt.Errorf("could not parse UnionDatasetSource: %v", sourceConfig)
				}
				return src, nil
			} else if sourceTypeName == "SampleSource" {
				src := &source.SampleSource{}
				src.Store = b.store
				numEntities := sourceConfig["numberofentities"]
				if numEntities != nil {
					src.NumberOfEntities = int(numEntities.(float64))
				}
				return src, nil
			} else if sourceTypeName == "SlowSource" {
				src := &source.SlowSource{}
				src.Sleep = sourceConfig["sleep"].(string)
				batch := sourceConfig["batchsize"]
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

func (b *pipelineBuilder) buildTransform(transformConfig map[string]any) (Transform, error) {
	// do a pass to lower case the keys
	for k, v := range transformConfig {
		transformConfig[strings.ToLower(k)] = v
	}

	if transformConfig != nil {
		transformTypeName := transformConfig["type"]
		if transformTypeName != nil {
			if transformTypeName == "HttpTransform" {
				transform := &HttpTransform{}
				url, ok := transformConfig["url"]
				if ok && url != "" {
					transform.Url = url.(string)
				}
				tokenProvider, ok := transformConfig["tokenprovider"]
				if ok {
					transform.TokenProvider = tokenProvider.(string)
				}
				timeout, ok := transformConfig["timeout"]
				if ok && timeout != 0 {
					transform.TimeOut = timeout.(float64)
				} else {
					transform.TimeOut = 0
				}
				transform.TokenProviders = b.tokenProviders
				return transform, nil
			} else if transformTypeName == "JavascriptTransform" {
				code64, ok := transformConfig["code"]
				if ok && code64 != "" {
					transform, err := newJavascriptTransform(b.logger, code64.(string), b.store)
					if err != nil {
						return nil, err
					}
					parallelism, ok := transformConfig["parallelism"]
					if ok {
						transform.Parallelism = int(parallelism.(float64))
						if err != nil {
							return nil, err
						}
					} else {
						transform.Parallelism = 1
					}
					transform.statsDClient = b.statsdClient
					return transform, nil
				}
				return nil, nil
			}
			return nil, errors.New("unknown transform type: " + transformTypeName.(string))
		}
		return nil, errors.New("transform config must contain 'Type'. can be one of: JavascriptTransform, HttpTransform")
	}
	return nil, nil
}
