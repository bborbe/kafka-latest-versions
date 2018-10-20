// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/run"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type App struct {
	KafkaBrokers string
	KafkaTopic   string
	Port         int
	DataDir      string
}

func (a *App) Validate() error {
	if a.KafkaBrokers == "" {
		return errors.New("KafkaBrokers missing")
	}
	if a.KafkaTopic == "" {
		return errors.New("KafkaTopic missing")
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
	db, err := bolt.Open(path.Join(a.DataDir, "bolt.db"), 0600, nil)
	if err != nil {
		glog.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("version")); err != nil {
			return fmt.Errorf("create bucket failed: %s", err)
		}
		glog.V(2).Infof("bucket version created")
		if _, err := tx.CreateBucketIfNotExists([]byte("offset")); err != nil {
			return fmt.Errorf("create bucket failed: %s", err)
		}
		glog.V(2).Infof("bucket offset created")
		return nil
	})
	if err != nil {
		glog.Fatal(err)
	}

	consumer := OffsetConsumer{
		KafkaTopic:   a.KafkaTopic,
		KafkaBrokers: a.KafkaBrokers,
		OffsetManager: &OffsetMessageHandler{
			DB: db,
		},
	}

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("Content-Type", "text/plain")
		resp.WriteHeader(http.StatusOK)
		err := db.View(func(tx *bolt.Tx) error {
			versionRegistry := VersionRegistry{
				Tx: tx,
			}
			return versionRegistry.ForEach(func(version avro.ApplicationVersionAvailable) error {
				fmt.Fprintf(resp, "%s = %s\n", version.App, version.Version)
				return nil
			})
		})
		if err != nil {
			glog.Warningf("read versions failed: %v", err)
		}
	})

	runServer := func(ctx context.Context) error {
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", a.Port),
			Handler: router,
		}
		go func() {
			select {
			case <-ctx.Done():
				server.Shutdown(ctx)
			}
		}()
		return server.ListenAndServe()
	}

	return run.CancelOnFirstFinish(ctx, consumer.Consume, runServer)
}
