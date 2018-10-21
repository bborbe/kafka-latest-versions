// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"fmt"
	"net/http"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

type AvailableHandler struct {
	DB *bolt.DB
}

func (a *AvailableHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Content-Type", "text/plain")
	resp.WriteHeader(http.StatusOK)
	err := a.DB.View(func(tx *bolt.Tx) error {
		availableRegistry := AvailableRegistry{
			Tx: tx,
		}
		return availableRegistry.ForEach(func(versionAvailable avro.ApplicationVersionAvailable) error {
			fmt.Fprintf(resp, "%s = %s\n", versionAvailable.App, versionAvailable.Version)
			return nil
		})
	})
	if err != nil {
		glog.Warningf("read versions failed: %v", err)
	}
}
