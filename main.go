// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	flag "github.com/bborbe/flagenv"
	"github.com/bborbe/kafka-latest-versions/version"
	"github.com/golang/glog"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())

	app := &version.App{}
	flag.StringVar(&app.DataDir, "datadir", "", "data directory")
	flag.StringVar(&app.KafkaBrokers, "kafka-brokers", "", "kafka brokers")
	flag.StringVar(&app.KafkaTopic, "kafka-topic", "", "kafka topic")
	flag.IntVar(&app.Port, "port", 9001, "port to listen")

	flag.Set("logtostderr", "true")
	flag.Parse()

	glog.V(0).Infof("Parameter datadir: %s", app.DataDir)
	glog.V(0).Infof("Parameter kafka-brokers: %s", app.KafkaBrokers)
	glog.V(0).Infof("Parameter kafka-topic: %s", app.KafkaTopic)
	glog.V(0).Infof("Parameter port: %d", app.Port)

	err := app.Validate()
	if err != nil {
		glog.Exit(err)
	}

	ctx := contextWithSig(context.Background())

	glog.V(0).Infof("app started")
	if err := app.Run(ctx); err != nil {
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
