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
	"encoding/base64"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/datahub/internal/conf"
	"github.com/mimiro-io/datahub/internal/security"
	"github.com/mimiro-io/internal-go-util/pkg/scheduler"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mimiro-io/datahub/internal/jobs/source"

	"github.com/franela/goblin"

	"github.com/mimiro-io/datahub/internal/server"
)

type pipelineTestSecurityProvider struct{}

func (p pipelineTestSecurityProvider) Authorize(req *http.Request) {
	// do nothing
}

var _ security.Provider = (*pipelineTestSecurityProvider)(nil)

func TestPipeline(t *testing.T) {
	g := goblin.Goblin(t)
	g.Describe("A pipeline", func() {
		testCnt := 0
		var dsm *server.DsManager
		var store *server.Store
		var runner *Runner
		var storeLocation string
		var mockService MockService
		var builder *pipelineBuilder
		g.BeforeEach(func() {
			// temp redirect of stdout and stderr to swallow some annoying init messages in fx and jobrunner and mockService
			devNull, _ := os.Open("/dev/null")
			oldErr := os.Stderr
			oldStd := os.Stdout
			os.Stderr = devNull
			os.Stdout = devNull

			testCnt += 1
			storeLocation = fmt.Sprintf("./testpipeline_%v", testCnt)
			err := os.RemoveAll(storeLocation)
			g.Assert(err).IsNil("should be allowed to clean testfiles in " + storeLocation)
			mockService = NewMockService()
			go func() {
				_ = mockService.echo.Start(":7777")
			}()
			_, store, runner, dsm, _ = setupScheduler(storeLocation, t)
			builder = newPipelineBuilder(&conf.Env{
				Logger: zap.NewNop().Sugar(),
			}, SchedulerParams{
				Store:    store,
				Dsm:      dsm,
				Runner:   runner,
				Statsd:   &statsd.NoOpClient{},
				EventBus: server.NoOpBus(),
				TokenProviders: &security.TokenProviders{
					Providers: &map[string]security.Provider{
						"local": security.BasicProvider{User: "u100", Password: "p200"},
					},
				},
			})

			// undo redirect of stdout and stderr after successful init of fx and jobrunner
			os.Stderr = oldErr
			os.Stdout = oldStd

		})
		g.AfterEach(func() {
			runner.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_ = mockService.echo.Shutdown(ctx)
			cancel()
			mockService.HttpNotificationChannel = nil
			_ = store.Close()
			_ = os.RemoveAll(storeLocation)
		})

		g.It("Should support internal js transform with txn writing to several datasets", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)
			_, _ = dsm.CreateDataset("ProductAudit", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// transform js
			js := `
			function transform_entities(entities) {
				for (e of entities) {
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
					txn.DatasetEntities["NewProducts"] = newentities;
					txn.DatasetEntities["ProductAudit"] = newentities;
					ExecuteTransaction(txn);
				}
				return entities;
			}
			`
			jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": jscriptEnc,
				},
				Sink: map[string]any{
					"Type": "DevNullSink",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)

			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("expected dataset is not present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")

			auditDataset := dsm.GetDataset("ProductAudit")
			g.Assert(auditDataset).IsNotNil("expected dataset is not present")

			result, err = auditDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")
		})

		g.It("Should support internal js transform with txn", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// transform js
			js := `
			function transform_entities(entities) {
				for (e of entities) {
					var txn = NewTransaction();
					var newentities = [];
					newentities.push(e);
					txn.DatasetEntities["NewProducts"] = newentities;
					ExecuteTransaction(txn);
				}
				return entities;
			}
			`
			jscriptEnc := base64.StdEncoding.EncodeToString([]byte(js))

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": jscriptEnc,
				},
				Sink: map[string]any{
					"Type": "DevNullSink",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)

			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("expected dataset is not present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("no result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "incorrect number of entities retrieved")
		})

		g.It("Should fullsync to an HttpDatasetSink", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)

			entities := make([]*server.Entity, 2)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			entity = server.NewEntity("http://data.mimiro.io/people/homer1", 0)
			entity.Properties["name"] = "homer"
			entities[1] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("dataset.StoreEntites returns no error")

			dsName := "fstohttp"

			task := &scheduler.TaskConfiguration{
				Id:        "sync-datasetssource-to-httpdatasetsink",
				BatchSize: 1,
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/" + dsName + "/fullsync",
				},
				Type: JobTypeFull,
			}
			pipeline, err := builder.buildV2(task)

			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			g.Assert(pipeline.spec().batchSize).Eql(1, "Batch size should be 1")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")
			g.Assert(len(mockService.getRecordedEntitiesForDataset(dsName))).Eql(2, "both 'pages' have been posted")
		})

		g.It("Should fullsync from an untokenized HttpDatasetSource", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People", nil)

			// define job
			task := &scheduler.TaskConfiguration{
				Id:        "sync-httpdatasetsource-to-datasetsink",
				BatchSize: 1,
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changes",
				},
				Type: JobTypeFull,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed running the pipeline")

			rs, err := ds.GetEntities("", 100)
			g.Assert(err).IsNil("we found data in sink")
			g.Assert(len(rs.Entities)).Eql(10, "we found 10 entites (MockService generates 10 results)")
		})

		g.It("Should fullsync from a tokenized HttpDatasetSource", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People", nil)

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-httpdatasetsource-to-datasetsink",
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changeswithcontinuation",
				},
				Type: JobTypeFull,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("jobConfig to Pipeline returns no error")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed the run")

			rs, err := ds.GetEntities("", 100)
			g.Assert(err).IsNil("we found data in sink")
			g.Assert(len(rs.Entities)).Eql(20, "we found 10 entites (MockService generates 20 results)")
		})

		//func TestDatasetToHttpDatasetSink(m *testing.T) {
		g.It("Should incrementally sync to an HttpDatasetSink", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("Entities are stored correctly")

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetssource-to-httpdatasetsink",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/writeabledevnull",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)

			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil()

		})

		g.It("Should incrementally do internal sync with js transform in parallel", func() {
			g.Timeout(time.Hour)
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			count := 19
			entities := make([]*server.Entity, count)
			for i := 0; i < count; i++ {
				entity := server.NewEntity("http://data.mimiro.io/people/p"+strconv.Itoa(i), 0)
				entity.Properties["name"] = "homer" + strconv.Itoa(i)
				entity.References["type"] = "http://data.mimiro.io/model/Person"
				entities[i] = entity
			}

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type":        "JavascriptTransform",
					"Code":        "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo=",
					"Parallelism": 10.0,
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewProducts",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run pipeline")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(19, "correct number of entities retrieved")
		})

		g.It("Should incrementally do internal sync with js transform in parallel when les than workers count", func() {
			g.Timeout(time.Hour)
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			entities := make([]*server.Entity, 2)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity
			entity1 := server.NewEntity("http://data.mimiro.io/people/marge", 0)
			entity1.Properties["name"] = "marge"
			entity1.References["type"] = "http://data.mimiro.io/model/Person"
			entities[1] = entity1

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo=",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewProducts",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(2, "correct number of entities retrieved")
		})

		//func TestDatasetToDatasetWithJavascriptTransformJob(m *testing.T) {
		g.It("Should incrementally do internal sync with js transform", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo=",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewProducts",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			g.Assert(peopleDataset).IsNotNil("dataset is present")

			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil("result is retrieved")

			g.Assert(len(result.Entities)).Eql(1, "correct number of entities retrieved")
		})
		g.It("Should run a transform with query in internal jobs", func() {
			testNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/test/")

			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People", nil)
			ds1, _ := dsm.CreateDataset("Companies", nil)
			_, _ = dsm.CreateDataset("NewPeople", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity(testNamespacePrefix+":gra", 0)
			entity.Properties[testNamespacePrefix+":name"] = "homer"
			entity.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Person"
			entity.References[testNamespacePrefix+":worksfor"] = testNamespacePrefix + ":mimiro"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			companies := make([]*server.Entity, 1)
			mimiro := server.NewEntity(testNamespacePrefix+":mimiro", 0)
			mimiro.Properties[testNamespacePrefix+":name"] = "Mimiro"
			mimiro.References[testNamespacePrefix+":type"] = testNamespacePrefix + ":Company"
			companies[0] = mimiro

			g.Assert(ds1.StoreEntities(companies)).IsNil()

			jsFun := `function transform_entities(entities) {
		    var test_ns = GetNamespacePrefix("http://data.mimiro.io/test/")
		    for (e of entities) {
		        Log(e["ID"])
		        var relatedCompanies = Query([ e["ID"] ], test_ns + ":worksfor", false);
				if (relatedCompanies.length == 1) {
					var firstCompany = relatedCompanies[0][2];
					Log(firstCompany);
					e["Properties"][test_ns + ":companyname"] = firstCompany["Properties"][test_ns + ":name"];
				}
		    }
		    return entities;
		}`
			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js-and-query",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "People",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": base64.StdEncoding.EncodeToString([]byte(jsFun)),
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewPeople",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewPeople")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
			g.Assert(result.Entities[0].Properties["ns3:companyname"]).Eql("Mimiro")
		})
		g.It("Should run a transform with subentities in internal jobs", func() {
			testNamespacePrefix, err := store.NamespaceManager.AssertPrefixMappingForExpansion("http://data.mimiro.io/test/")

			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("People", nil)
			_, _ = dsm.CreateDataset("NewPeople", nil)

			address := server.NewEntity(testNamespacePrefix+":home", 0)
			address.Properties[testNamespacePrefix+":street"] = "homestreet"

			entities := make([]*server.Entity, 2)

			entities[0] = server.NewEntity(testNamespacePrefix+":homer", 0)
			entities[0].Properties[testNamespacePrefix+":name"] = "homer"
			entities[0].Properties[testNamespacePrefix+":address"] = address
			entities[0].Properties[testNamespacePrefix+":listref"] = []string{"//homer", "//male"}
			entities[0].Properties[testNamespacePrefix+":ref"] = "//homer"

			entities[1] = server.NewEntity(testNamespacePrefix+":barney", 0)
			entities[1].Properties[testNamespacePrefix+":name"] = "barney"
			entities[1].Properties[testNamespacePrefix+":address"] = map[string]interface{}{
				"id":    testNamespacePrefix + ":barn",
				"props": map[string]interface{}{testNamespacePrefix + ":street": "barnstreet"},
				"refs":  map[string]interface{}{},
			}

			g.Assert(ds.StoreEntities(entities)).IsNil()

			jsFun := `function transform_entities(entities) {
		    var test_ns = GetNamespacePrefix("http://data.mimiro.io/test/");
			var result = [];
		    for (e of entities) {
                var address = GetProperty(e, test_ns, "address");
				var listref = GetProperty(e, test_ns, "listref");
				var ref = GetProperty(e, test_ns, "ref");
                //sub-entities must be converted to Entity instances before GetProperty and other helpers work on them
                var addressEntity = AsEntity(address)
                var street = GetProperty(addressEntity, test_ns, "street");
                var r = NewEntity();
				SetId(r, GetId(e));
	            SetProperty(r, test_ns,"street",street);
	            SetProperty(r, test_ns,"address",AsEntity(address));
	            SetProperty(r, test_ns,"no_address",AsEntity(street));
				AddReference(r, test_ns, "listref", "listref")
				AddReference(r, test_ns, "ref", "ref")
                result.push(r);
		    }

		    return result;
		}`

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js-and-query",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "People",
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": base64.StdEncoding.EncodeToString([]byte(jsFun)),
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewPeople",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewPeople")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(2)
			g.Assert(result.Entities[0].Properties["ns3:street"]).Eql("homestreet")
			g.Assert(result.Entities[0].Properties["ns3:no_address"]).IsNil()
			g.Assert(result.Entities[0].Properties["ns3:address"]).IsNotNil()
			g.Assert(result.Entities[1].Properties["ns3:street"]).Eql("barnstreet")
		})

		g.It("Should run external transforms in internal jobs", func() {
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Transform: map[string]any{
					"Type": "HttpTransform",
					"Url":  "http://localhost:7777/transforms/identity",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewProducts",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should not write to the sink if an external transform endpoint is 404", func() {
			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-1",
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasetsarecool/people/changes",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())

			g.Assert(err).IsNotNil("should have failed")
			g.Assert(dsm.GetDataset("People")).IsZero("dataset should not exist as job failed")
		})

		g.It("Should copy datasetsource to datasetsink in first run of internal job", func() {
			ds, _ := dsm.CreateDataset("Products", nil)
			_, _ = dsm.CreateDataset("NewProducts", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entity.References["type"] = "http://data.mimiro.io/model/Person"
			entity.References["typed"] = "http://data.mimiro.io/model/Person"
			entities[0] = entity

			g.Assert(ds.StoreEntities(entities)).IsNil()

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "Products",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "NewProducts",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// check number of entities in target dataset
			peopleDataset := dsm.GetDataset("NewProducts")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should copy from samplesource to datasetsink in first run of new job", func() {
			_, _ = dsm.CreateDataset("People", nil)

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink",
				Source: map[string]any{
					"Type":             "SampleSource",
					"NumberOfEntities": 1.0,
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(1)
		})

		g.It("Should copy from HttpDatasetSource to datasetSink in first run of new job", func() {
			_, _ = dsm.CreateDataset("People", nil)

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-1",
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changes",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(10)
		})
		g.It("Should copy all pages using continuatin tokens from httpDatasetSource to datasetSink if first run", func() {
			_, _ = dsm.CreateDataset("People", nil)

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-1",
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changeswithcontinuation",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "People",
				},
				Type: JobTypeIncremental,
			}

			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil("pipeline is parsed correctly")

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			// get entities from people dataset
			peopleDataset := dsm.GetDataset("People")
			result, err := peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(20)

			// run again
			_ = pipeline.sync(context.Background())
			peopleDataset = dsm.GetDataset("People")
			result, err = peopleDataset.GetEntities("", 50)
			g.Assert(err).IsNil()
			g.Assert(len(result.Entities)).Eql(20, "results should stay the same")
		})

		g.It("Should mark entities that have not been received again during fullsync to internal dataset as deleted", func() {
			g.Timeout(1 * time.Hour)
			sourceDs, _ := dsm.CreateDataset("people", nil)
			sinkDs, _ := dsm.CreateDataset("people2", nil)

			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = sourceDs.StoreEntities([]*server.Entity{e1, e2})

			task := &scheduler.TaskConfiguration{
				Id: "fullsync-1x",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "people",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "people2",
				},
				Type: JobTypeFull,
			}

			pipeline, err := builder.buildV2(task)

			// run once, both entities should sync
			_ = pipeline.sync(context.Background())

			res, err := sinkDs.GetEntities("", 100)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(2)
			g.Assert(res.Entities[0].ID).Eql("1")
			g.Assert(res.Entities[0].IsDeleted).Eql(false)
			g.Assert(res.Entities[1].ID).Eql("2")
			g.Assert(res.Entities[1].IsDeleted).Eql(false)

			// delete ds and recreate with only 1 entity
			g.Assert(dsm.DeleteDataset("people")).IsNil()
			sourceDs, _ = dsm.CreateDataset("people", nil)
			g.Assert(sourceDs.StoreEntities([]*server.Entity{e2})).IsNil()

			// run again. deletion detection should apply
			_ = pipeline.sync(context.Background())

			res, err = sinkDs.GetEntities("", 100)
			g.Assert(err).IsNil()
			g.Assert(len(res.Entities)).Eql(2)
			g.Assert(res.Entities[0].ID).Eql("1")
			g.Assert(res.Entities[0].IsDeleted).Eql(true, "Entity 1 should be deleted now")
			g.Assert(res.Entities[1].ID).Eql("2")
			g.Assert(res.Entities[1].IsDeleted).Eql(false)

		})
		g.It("Should store continuation token after every page in incremental job", func() {
			task := &scheduler.TaskConfiguration{
				Id:        "inc-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type":             "SampleSource",
					"NumberOfEntities": 10.0,
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/inctest/fullsync",
				},
				Type: JobTypeIncremental,
			}
			pipeline, _ := builder.buildV2(task)

			//run async, so we can verify tokens in parallel
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				_ = pipeline.sync(context.Background())
				wg.Done()
			}()

			//block and wait for channel notification - indicating the first page/batch has been received
			_ = <-mockService.HttpNotificationChannel
			g.Assert(len(mockService.getRecordedEntitiesForDataset("inctest"))).
				Eql(5, "After first batch, 5 entities should have been postet to httpSink")

			//block for next batch request finished - this should be before syncState is updated
			_ = <-mockService.HttpNotificationChannel
			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("5", "Between batch 1 and 2, token should be continuation of batch 1")

			wg.Wait()
			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("10")
		})
		g.It("Should store continuation token only after finished run in fullsync job", func() {
			task := &scheduler.TaskConfiguration{
				Id:        "full-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type":             "SampleSource",
					"NumberOfEntities": 10.0,
				},
				Sink: map[string]any{
					"Type": "DevNullSink",
				},
				Type: JobTypeFull,
			}
			pipeline, _ := builder.buildV2(task)

			//run async, so we can verify tokens in parallel
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				_ = pipeline.sync(context.Background())
				wg.Done()
			}()
			//block and wait for channel notification - indicating the first page/batch has been received
			//_ = <-mockService.HttpNotificationChannel
			//g.Assert(len(mockService.getRecordedEntitiesForDataset("fulltest"))).
			//	Eql(5, "After first batch, 5 entities should have been postet to httpSink")

			//wait for first syncState (token) update in badger (should be in db when 2nd batch arrives)
			//_ = <-mockService.HttpNotificationChannel
			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("", "there should not be a token stored after first batch")

			//wait for job to finish
			wg.Wait()
			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("10", "First after job there should be a token")
		})

		g.It("Should post changes to HttpDatasetSink endpoint if jobType is incremental", func() {
			srcDs, _ := dsm.CreateDataset("src", nil)
			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
			e1.IsDeleted = true
			_ = srcDs.StoreEntities([]*server.Entity{e1})
			e1.IsDeleted = false
			_ = srcDs.StoreEntities([]*server.Entity{e1})

			var sourceChanges []*server.Entity
			_, err := srcDs.ProcessChanges(0, 100, false, func(entity *server.Entity) {
				sourceChanges = append(sourceChanges, entity)
			})
			g.Assert(err).IsNil()
			g.Assert(len(sourceChanges)).Eql(4, "Expected 4 changes for our two entities in source")

			task := &scheduler.TaskConfiguration{
				Id:        "inc-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "src",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/inctest/fullsync",
				},
				Type: JobTypeIncremental,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())
			sinkChanges := mockService.getRecordedEntitiesForDataset("inctest")
			g.Assert(len(sinkChanges)).Eql(4, "Expected all 4 changes in sink for incremental")
		})

		g.It("Should post entities to HttpDatasetSink endpoint if jobType is fullsync", func() {
			srcDs, _ := dsm.CreateDataset("src", nil)
			e1 := server.NewEntity("1", 0)
			e2 := server.NewEntity("2", 0)
			_ = srcDs.StoreEntities([]*server.Entity{e1, e2})
			e1.IsDeleted = true
			_ = srcDs.StoreEntities([]*server.Entity{e1})
			e1.IsDeleted = false
			_ = srcDs.StoreEntities([]*server.Entity{e1})

			var sourceChanges []*server.Entity
			_, err := srcDs.ProcessChanges(0, 100, false, func(entity *server.Entity) {
				sourceChanges = append(sourceChanges, entity)
			})
			g.Assert(err).IsNil()
			g.Assert(len(sourceChanges)).Eql(4, "Expected 4 changes for our two entities in source")

			task := &scheduler.TaskConfiguration{
				Id:        "inc-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "src",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/fulltest/fullsync",
				},
				Type: JobTypeFull,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())
			sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(2, "Expected only 2 entities in current state in fullsync")

		})

		g.It("Should store dependency watermarks after fullsync in MultiSource jobs", func() {
			srcDs, _ := dsm.CreateDataset("src", nil)
			_ = srcDs.StoreEntities([]*server.Entity{
				server.NewEntity("1", 0),
				server.NewEntity("2", 0)})

			depDs, _ := dsm.CreateDataset("dep", nil)
			_ = depDs.StoreEntities([]*server.Entity{
				server.NewEntity("3", 0),
				server.NewEntity("4", 0),
				server.NewEntity("5", 0)})

			task := &scheduler.TaskConfiguration{
				Id:        "fs-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type": "MultiSource",
					"Name": "src",
					"Dependencies": []any{
						map[string]any{
							"dataset": "dep",
							"joins": []any{
								map[string]any{
									"dataset":   "src",
									"predicate": "http:/a/predicate",
									"inverse":   false,
								},
							},
						},
					},
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/fulltest/fullsync",
				},
				Type: JobTypeFull,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(2, "Expected only 2 entities in current state in fullsync")

			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"3\"}}}",
				"after job there should be a token")

		})

		g.It("Should store dependency watermarks after incremental in MultiSource jobs", func() {
			g.Timeout(1 * time.Hour)
			srcDs, _ := dsm.CreateDataset("src", nil)
			ns, _ := store.NamespaceManager.AssertPrefixMappingForExpansion("http://namespace/")
			_ = srcDs.StoreEntities([]*server.Entity{
				server.NewEntity(ns+":1", 0),
				server.NewEntity(ns+":2", 0)})

			depDs, _ := dsm.CreateDataset("dep", nil)
			_ = depDs.StoreEntities([]*server.Entity{
				server.NewEntity(ns+":3", 0),
				server.NewEntity(ns+":4", 0),
				server.NewEntity(ns+":5", 0)})

			/*pipeline := &IncrementalPipeline{PipelineSpec{
				batchSize: 5,
				source: &source.MultiSource{DatasetName: "src", Store: store, DatasetManager: dsm, Dependencies: []source.Dependency{
					{Dataset: "dep", Joins: []source.Join{{Dataset: "src", Predicate: ns + ":predicate", Inverse: false}}},
				}},
				sink: &httpDatasetSink{Endpoint: "http://localhost:7777/datasets/fulltest/fullsync", Store: store},
			}}
			job := &job{id: "fs-1", pipeline: pipeline, runner: runner}
			job.Run()*/

			task := &scheduler.TaskConfiguration{
				Id:        "fs-1",
				BatchSize: 5,
				Source: map[string]any{
					"Type": "MultiSource",
					"Name": "src",
					"Dependencies": []any{
						map[string]any{
							"dataset": "dep",
							"joins": []any{
								map[string]any{
									"dataset":   "src",
									"predicate": ns + ":predicate",
									"inverse":   false,
								},
							},
						},
					},
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/fulltest/fullsync",
				},
				Type: JobTypeIncremental,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(2, "Expected only 2 entities")

			syncJobState := &SyncJobState{}
			err := store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"3\"}}}",
				"after job there should be a token")

			//reset recorder
			for k, _ := range mockService.RecordedEntities {
				delete(mockService.RecordedEntities, k)
			}
			//add dependency link
			e := server.NewEntity(ns+":5", 0)
			e.References[ns+":predicate"] = ns + ":1"
			_ = depDs.StoreEntities([]*server.Entity{e})

			_ = pipeline.sync(context.Background())

			sinkChanges = mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(1, "Expected only 1 entity (id=1 was linked to by dependency)")
			g.Assert(sinkChanges[0].ID).Eql(ns+":1", "new dependency points to id 1")

			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"4\"}}}",
				"dependency watermarks should be forwarded by 1")

			//reset recorder
			for k, _ := range mockService.RecordedEntities {
				delete(mockService.RecordedEntities, k)
			}

			// run one more time without changes to source data, make sure nothing is done
			_ = pipeline.sync(context.Background())

			sinkChanges = mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(0)

			syncJobState = &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token = syncJobState.ContinuationToken
			g.Assert(token).Eql("{\"MainToken\":\"2\",\"DependencyTokens\":{\"dep\":{\"Token\":\"4\"}}}",
				"dependency watermarks should be unchanged")
		})
		g.It("Should not store UnionDatasetContinuation after fullsync on UnionDatasetSource", func() {
			ds1, _ := dsm.CreateDataset("src1", nil)
			_ = ds1.StoreEntities([]*server.Entity{
				server.NewEntity("1", 0),
				server.NewEntity("2", 0)})
			ds2, _ := dsm.CreateDataset("src2", nil)
			_ = ds2.StoreEntities([]*server.Entity{
				server.NewEntity("3", 0),
				server.NewEntity("4", 0),
				server.NewEntity("5", 0)})

			task := &scheduler.TaskConfiguration{
				Id: "fs-1",
				Source: map[string]any{
					"Type": "UnionDatasetSource",
					"DatasetSources": []any{
						map[string]any{
							"Name": "src1",
						},
						map[string]any{
							"Name": "src2",
						},
					},
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/fulltest/fullsync",
				},
				Type: JobTypeFull,
			}
			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil()
			_ = pipeline.sync(context.Background())

			sinkChanges := mockService.getRecordedEntitiesForDataset("fulltest")
			g.Assert(len(sinkChanges)).Eql(5, "Expected 5 entities in current state in fullsync")

			syncJobState := &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("",
				"after job there should be NO token because fullsync uses entities towards httpsink")

		})
		g.It("Should store UnionDatasetContinuation after incremental on UnionDatasetSource", func() {
			ds1, _ := dsm.CreateDataset("src1", nil)
			_ = ds1.StoreEntities([]*server.Entity{
				server.NewEntity("1", 0),
				server.NewEntity("2", 0)})
			ds2, _ := dsm.CreateDataset("src2", nil)
			_ = ds2.StoreEntities([]*server.Entity{
				server.NewEntity("3", 0),
				server.NewEntity("4", 0),
				server.NewEntity("5", 0)})

			task := &scheduler.TaskConfiguration{
				Id: "inc-1",
				Source: map[string]any{
					"Type": "UnionDatasetSource",
					"DatasetSources": []any{
						map[string]any{
							"Name": "src1",
						},
						map[string]any{
							"Name":       "src2",
							"LatestOnly": true,
						},
					},
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/inctest/fullsync",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil()
			err = pipeline.sync(context.Background())
			g.Assert(err).IsNil("failed to run")

			sinkChanges := mockService.getRecordedEntitiesForDataset("inctest")
			g.Assert(len(sinkChanges)).Eql(5, "Expected 5 entities in current state")

			syncJobState := &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			token := syncJobState.ContinuationToken
			g.Assert(token).Eql("{\"Tokens\":[{\"Token\":\"2\"},{\"Token\":\"3\"}],\"DatasetNames\":[\"src1\",\"src2\"]}",
				"after job there should be a token stored")

		})
		g.It("Should fail run and return error when error in transform js", func() {
			// populate dataset with some entities
			ds, _ := dsm.CreateDataset("Products", nil)

			entities := make([]*server.Entity, 1)
			entity := server.NewEntity("http://data.mimiro.io/people/homer", 0)
			entity.Properties["name"] = "homer"
			entities[0] = entity

			err := ds.StoreEntities(entities)
			g.Assert(err).IsNil("entities are stored")

			// transform js
			js := `
			function transform_entities(entities) {
				for (e of entities) {
					var something = null;
					var fail = something[0][2];
				}
				return entities;
			}
			`

			task := &scheduler.TaskConfiguration{
				Id: "sync-datasetsource-to-datasetsink-with-js",
				Source: map[string]any{
					"Type":       "DatasetSource",
					"Name":       "Products",
					"LatestOnly": true,
				},
				Transform: map[string]any{
					"Type": "JavascriptTransform",
					"Code": base64.StdEncoding.EncodeToString([]byte(js)),
				},
				Sink: map[string]any{
					"Type": "DevNullSink",
				},
				Type: JobTypeIncremental,
			}
			pipeline, err := builder.buildV2(task)
			g.Assert(err).IsNil()
			g.Assert(pipeline.spec().source.(*source.DatasetSource).LatestOnly).IsTrue()

			err = pipeline.sync(context.Background())
			g.Assert(err).IsNotNil()

			syncJobState := &SyncJobState{}
			err = store.GetObject(server.JOB_DATA_INDEX, task.Id, syncJobState)
			g.Assert(err).IsNil()
			g.Assert(syncJobState.LastRunCompletedOk).IsFalse()

		})

		g.It("Should support proxy dataset as incremental datasetSource", func() {
			_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
				ProxyDatasetConfig: &server.ProxyDatasetConfig{
					RemoteUrl: "http://localhost:7777/datasets/people"}})
			g.Assert(err).IsNil()

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-proxydatasetsource-to-httpdatasetsink",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "proxy",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/proxysink/fullsync",
				},
				Type: JobTypeIncremental,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			g.Assert(len(mockService.RecordedEntities["proxysink"])).Eql(11, "sink received content")
			g.Assert(mockService.RecordedEntities["proxysink"][1].ID).Eql("ns3:e-0", "sink received entity from proxy")
		})

		g.It("Should support proxy dataset as fullsync datasetSource", func() {
			_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
				ProxyDatasetConfig: &server.ProxyDatasetConfig{
					RemoteUrl: "http://localhost:7777/datasets/people"}})
			g.Assert(err).IsNil()

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-proxydatasetsource-to-httpdatasetsink",
				Source: map[string]any{
					"Type": "DatasetSource",
					"Name": "proxy",
				},
				Sink: map[string]any{
					"Type": "HttpDatasetSink",
					"Url":  "http://localhost:7777/datasets/proxysink/fullsync",
				},
				Type: JobTypeFull,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			ents := mockService.getRecordedEntitiesForDataset("proxysink")
			g.Assert(len(ents)).Eql(10, "sink received content")
			g.Assert(ents[0].ID).Eql("ns3:fs-0", "sink received entity from proxy")
		})

		g.It("Should support proxy dataset as incremental datasetSink", func() {
			_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
				ProxyDatasetConfig: &server.ProxyDatasetConfig{
					RemoteUrl:        "http://localhost:7777/datasets/people",
					AuthProviderName: "local",
				}})
			g.Assert(err).IsNil()

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-proxydatasetsource-to-httpdatasetsink",
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changeswithcontinuation",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "proxy",
				},
				Type: JobTypeIncremental,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			var receivedMockRequests []*http.Request
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for afterCh := time.After(100 * time.Millisecond); ; {
					select {
					case d := <-mockService.HttpNotificationChannel:
						receivedMockRequests = append(receivedMockRequests, d)
					case <-afterCh:
						return
					}
				}
			}()
			wg.Wait()
			g.Assert(len(receivedMockRequests) > 0).IsTrue()
			firstSinkReq := receivedMockRequests[1]
			g.Assert(firstSinkReq.Header["Authorization"]).Eql([]string{"Basic dTEwMDpwMjAw"})
			ents := mockService.getRecordedEntitiesForDataset("people")
			g.Assert(len(receivedMockRequests)).Eql(4, "2 requests to source and 2 to sink")
			g.Assert(len(ents)).Eql(20, "sink received 2 batches of content")
			g.Assert(ents[10].ID).Eql("ns3:e-0")
		})

		g.It("Should support proxy dataset as fullsync datasetSink", func() {
			_, err := dsm.CreateDataset("proxy", &server.CreateDatasetConfig{
				ProxyDatasetConfig: &server.ProxyDatasetConfig{
					RemoteUrl:        "http://localhost:7777/datasets/people",
					AuthProviderName: "local",
				}})
			g.Assert(err).IsNil()

			// define job
			task := &scheduler.TaskConfiguration{
				Id: "sync-proxydatasetsource-to-httpdatasetsink",
				Source: map[string]any{
					"Type": "HttpDatasetSource",
					"Url":  "http://localhost:7777/datasets/people/changeswithcontinuation",
				},
				Sink: map[string]any{
					"Type": "DatasetSink",
					"Name": "proxy",
				},
				Type: JobTypeFull,
			}
			pipeline, _ := builder.buildV2(task)
			_ = pipeline.sync(context.Background())

			var receivedMockRequests []*http.Request
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for afterCh := time.After(100 * time.Millisecond); ; {
					select {
					case d := <-mockService.HttpNotificationChannel:
						receivedMockRequests = append(receivedMockRequests, d)
					case <-afterCh:
						return
					}
				}
			}()
			wg.Wait()

			g.Assert(len(receivedMockRequests) > 0).IsTrue()
			firstSinkReq := receivedMockRequests[1]
			g.Assert(firstSinkReq.Header["Authorization"]).Eql([]string{"Basic dTEwMDpwMjAw"})
			g.Assert(firstSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).IsNotZero()
			g.Assert(firstSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).Eql([]string{"true"})
			g.Assert(firstSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).IsZero()
			secondSinkReq := receivedMockRequests[3]
			g.Assert(secondSinkReq.Header["Authorization"]).Eql([]string{"Basic dTEwMDpwMjAw"})
			g.Assert(secondSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).IsNotZero()
			g.Assert(secondSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).IsZero()
			g.Assert(secondSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).IsZero()
			thirdSinkReq := receivedMockRequests[4]
			g.Assert(thirdSinkReq.Header["Authorization"]).Eql([]string{"Basic dTEwMDpwMjAw"})
			g.Assert(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-Id"]).IsNotZero()
			g.Assert(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-Start"]).IsZero()
			g.Assert(thirdSinkReq.Header["Universal-Data-Api-Full-Sync-End"]).Eql([]string{"true"})
			ents := mockService.getRecordedEntitiesForDataset("people")
			g.Assert(len(receivedMockRequests)).Eql(5, "2 requests to source and 2 to sink plus fullsync end to sink")
			g.Assert(len(ents)).Eql(20, "sink received 2 batches of content")
			g.Assert(ents[10].ID).Eql("ns3:e-0")
		})

		// FIXME: disabled because of incompatible changes with new scheduler
		/*
			g.It("Should not panic when pipeline sink dataset is missing", func() {
				// define job where datasets in source and sink are missing
				jobJson := `
				{
					"id" : "sync-multi-to-dataset",
					"triggers": [{"triggerType": "cron", "jobType": "fullSync", "schedule": "@every 2s"}],
					"source" : {
						"Type" : "MultiSource",
						"Name" : "people", "Dependencies": [ {
						"dataset": "address",
						"joins": [
							{ "dataset": "office", "predicate": "http://office/location", "inverse": true },
							{ "dataset": "people", "predicate": "http://office/contact", "inverse": false },
							{ "dataset": "team", "predicate": "http://team/lead", "inverse": true },
							{ "dataset": "people", "predicate": "http://team/member", "inverse": false }
						] } ]
					},
					"sink" : {
						"Type" : "DatasetSink",
						"Name" : "testsink"
					}
				}`

				jobConfig, _ := scheduler.Parse([]byte(jobJson))
				pipeline, err := scheduler.toPipeline(jobConfig, JobTypeFull)
				g.Assert(err).IsNil("pipeline is parsed correctly")

				job := &job{
					id:       jobConfig.Id,
					pipeline: pipeline,
					schedule: jobConfig.Triggers[0].Schedule,
					runner:   runner,
				}

				job.Run()
				history := scheduler.GetJobHistory()
				g.Assert(history[0].LastError).Eql("dataset does not exist: testsink")

				// fix sink, now missing source should not panic
				_, _ = dsm.CreateDataset("testsink", nil)
				job.Run()
				history = scheduler.GetJobHistory()
				g.Assert(history[0].LastError).Eql("dataset people is missing, dataset is missing")

				// fix main source as well, errors should be gone
				_, _ = dsm.CreateDataset("people", nil)
				job.Run()
				history = scheduler.GetJobHistory()
				g.Assert(history[0].LastError).IsZero()
			}) */
	})
}
