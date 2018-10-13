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
	"runtime"
	"syscall"

	flag "github.com/bborbe/flagenv"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/kafka-latest-versions/version"
	"github.com/bborbe/run"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seibert-media/go-kafka/consumer"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())

	versionUpdates := make(chan avro.Version, runtime.NumCPU())

	kafkaConsumer := &consumer.SimpleConsumer{
		MessageHandler: &version.MessageHandler{
			VersionUpdates: versionUpdates,
		},
	}
	versionStore := &version.Store{
		VersionUpdates: versionUpdates,
	}

	flag.StringVar(&kafkaConsumer.KafkaBrokers, "kafka-brokers", "", "kafka brokers")
	flag.StringVar(&kafkaConsumer.KafkaTopic, "kafka-topic", "", "kafka topic")
	portPtr := flag.Int("port", 9001, "port to listen")

	flag.Set("logtostderr", "true")
	flag.Parse()

	if kafkaConsumer.KafkaBrokers == "" {
		glog.Exitf("KafkaBrokers missing")
	}
	if kafkaConsumer.KafkaTopic == "" {
		glog.Exitf("KafkaTopic missing")
	}

	ctx, cancel := context.WithCancel(contextWithSig(context.Background()))
	go func() {
		err := versionStore.Import(ctx)
		if err != nil {
			glog.Warningf("import versions failed: %v", err)
			cancel()
		}
	}()

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("Content-Type", "text/plain")
		resp.WriteHeader(http.StatusOK)
		for _, v := range versionStore.Latest() {
			fmt.Fprintf(resp, "%s = %s\n", v.App, v.Number)
		}
	})

	runServer := func(ctx context.Context) error {
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", *portPtr),
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

	glog.V(0).Infof("app started")
	if err := run.CancelOnFirstFinish(ctx, kafkaConsumer.Consume, runServer); err != nil {
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

func b() {
	db, err := bolt.Open("accounts.db", 0600, nil)
	if err != nil {
		glog.Fatal(err)
	}
	defer db.Close()

	// read offset from bold
	// read version topic
	// save read data to bolt
	// save offset to bold

	// rest endpoint sprint latest versions

	id := "123"
	bucket := "AccountBucket"

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("create bucket failed: %s", err)
		}
		return nil
	})
	if err != nil {
		glog.Fatal(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		return b.Put([]byte(id), []byte("hello world"))
	})
	if err != nil {
		glog.Fatal(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		content := b.Get([]byte(id))
		os.Stdout.Write(content)
		return nil
	})
	if err != nil {
		glog.Fatal(err)
	}

}
