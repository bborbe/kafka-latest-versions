// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/bborbe/run"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seibert-media/go-kafka/persistent"
	"github.com/seibert-media/go-kafka/schema"
)

type App struct {
	DataDir                    string
	KafkaAvailableVersionTopic string
	KafkaBrokers               string
	KafkaLatestVersionTopic    string
	KafkaSchemaRegistryUrl     string
	Port                       int
}

func (a *App) Validate() error {
	if a.KafkaBrokers == "" {
		return errors.New("KafkaBrokers missing")
	}
	if a.KafkaAvailableVersionTopic == "" {
		return errors.New("KafkaAvailableVersionTopic missing")
	}
	if a.KafkaLatestVersionTopic == "" {
		return errors.New("KafkaLatestVersionTopic missing")
	}
	if a.KafkaSchemaRegistryUrl == "" {
		return errors.New("SchemaRegistryUrl missing")
	}
	if a.DataDir == "" {
		return errors.New("DataDir missing")
	}
	if a.Port <= 0 {
		return errors.New("Port invalid")
	}
	return nil
}

func (a *App) Run(ctx context.Context) error {
	db, err := bolt.Open(path.Join(a.DataDir, "kafka-latest-versions.db"), 0600, nil)
	if err != nil {
		return errors.Wrap(err, "open bolt db failed")
	}
	defer db.Close()

	consumer, err := a.createConsumer(db)
	if err != nil {
		return errors.Wrap(err, "create consumer failed")
	}
	runHttpServer := func(ctx context.Context) error {
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", a.Port),
			Handler: a.createHttpHandler(db),
		}
		go func() {
			select {
			case <-ctx.Done():
				server.Shutdown(ctx)
			}
		}()
		return server.ListenAndServe()
	}

	return run.CancelOnFirstFinish(ctx, consumer.Consume, runHttpServer)
}

func (a *App) createHttpHandler(db *bolt.DB) http.Handler {
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.Handle("/", &AvailableHandler{
		DB: db,
	})
	return router
}

func (a *App) createConsumer(db *bolt.DB) (*persistent.Consumer, error) {
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
		return nil, errors.Wrap(err, "create default bolt buckets failed")
	}
	return &persistent.Consumer{
		KafkaTopic:   a.KafkaAvailableVersionTopic,
		KafkaBrokers: a.KafkaBrokers,
		OffsetManager: &persistent.MessageHandler{
			DB: db,
			MessageHandler: &MessageHandler{
				LatestVersionPublisher: &Publisher{
					KafkaBrokers: a.KafkaBrokers,
					KafkaTopic:   a.KafkaLatestVersionTopic,
					SchemaRegistry: &schema.Registry{
						HttpClient:        http.DefaultClient,
						SchemaRegistryUrl: a.KafkaSchemaRegistryUrl,
					},
				},
			},
			OffsetBucketName: offsetBucketName,
		},
	}, nil
}
