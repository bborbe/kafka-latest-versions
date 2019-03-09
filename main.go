// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"

	"github.com/bborbe/argument"

	flag "github.com/bborbe/flagenv"
	"github.com/bborbe/kafka-latest-versions/latestversion"
	"github.com/bborbe/run"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seibert-media/go-kafka/persistent"
	"github.com/seibert-media/go-kafka/schema"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())
	_ = flag.Set("logtostderr", "true")

	app := &application{}
	if err := argument.Parse(app); err != nil {
		glog.Exitf("parse app failed: %v", err)
	}

	glog.V(0).Infof("app started")
	if err := app.run(contextWithSig(context.Background())); err != nil {
		glog.Exitf("app failed: %+v", err)
	}
	glog.V(0).Infof("app finished")
}

func contextWithSig(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-signalCh:
		case <-ctx.Done():
		}
	}()

	return ctxWithCancel
}

type application struct {
	DataDir                    string `required:"true" arg:"datadir" env:"DATADIR" default:"" usage:"data directory"`
	KafkaAvailableVersionTopic string `required:"true" arg:"kafka-available-version-topic" env:"KAFKA_AVAILABLE_VERSION_TOPIC" usage:"Kafka topic"`
	KafkaBrokers               string `required:"true" arg:"kafka-brokers" env:"KAFKA_BROKERS" usage:"Kafka brokers"`
	KafkaLatestVersionTopic    string `required:"true" arg:"kafka-latest-version-topic" env:"KAFKA_LATEST_VERSION_TOPIC" usage:"Kafka topic"`
	KafkaSchemaRegistryUrl     string `required:"true" arg:"kafka-schema-registry-url" env:"KAFKA_SCHEMA_REGISTRY_URL" usage:"Kafka schema registry url"`
	Port                       int    `required:"true" arg:"port" env:"PORT" default:"9004" usage:"Port to listen"`
}

func (a *application) run(ctx context.Context) error {
	db, err := bolt.Open(path.Join(a.DataDir, "kafka-latest-versions.db"), 0600, nil)
	if err != nil {
		return errors.Wrap(err, "open bolt db failed")
	}
	defer db.Close()

	return run.CancelOnFirstFinish(
		ctx,
		a.createConsumer(db),
		a.createHttpServer(db),
	)
}

func (a *application) createHttpServer(db *bolt.DB) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", a.Port),
			Handler: a.createHttpHandler(db),
		}
		go func() {
			select {
			case <-ctx.Done():
				if err := server.Shutdown(ctx); err != nil {
					glog.Warningf("shutdown failed: %v", err)
				}
			}
		}()
		return server.ListenAndServe()
	}
}

func (a *application) createHttpHandler(db *bolt.DB) http.Handler {
	router := mux.NewRouter()
	router.HandleFunc("/healthz", a.check)
	router.HandleFunc("/readiness", a.check)
	router.Handle("/metrics", promhttp.Handler())
	router.Handle("/versions", &latestversion.AvailableHandler{
		DB: db,
	})
	router.Handle("/", &latestversion.IndexHandler{})
	return router
}

func (a *application) check(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusOK)
}

func (a *application) createConsumer(db *bolt.DB) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		offsetBucketName := []byte("offset")
		err := db.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucketIfNotExists([]byte("version")); err != nil {
				return fmt.Errorf("create bucket failed: %s", err)
			}
			glog.V(2).Infof("bucket version created")
			if _, err := tx.CreateBucketIfNotExists(offsetBucketName); err != nil {
				return fmt.Errorf("create bucket failed: %s", err)
			}
			glog.V(2).Infof("bucket offset created")
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "create default bolt buckets failed")
		}
		consumer := &persistent.Consumer{
			KafkaTopic:   a.KafkaAvailableVersionTopic,
			KafkaBrokers: a.KafkaBrokers,
			OffsetManager: &persistent.MessageHandler{
				DB: db,
				MessageHandler: &latestversion.MessageHandler{
					LatestVersionPublisher: &latestversion.Publisher{
						KafkaBrokers: a.KafkaBrokers,
						KafkaTopic:   a.KafkaLatestVersionTopic,
						SchemaRegistry: schema.NewRegistry(
							http.DefaultClient,
							a.KafkaSchemaRegistryUrl,
						),
					},
				},
				OffsetBucketName: offsetBucketName,
			},
		}
		return consumer.Consume(ctx)
	}
}
