// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"encoding/json"
	"net/http"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

func NewAvailableHandler(db *bolt.DB) http.Handler {
	return &availableHandler{
		db: db,
	}
}

type availableHandler struct {
	db *bolt.DB
}

func (a *availableHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	list := make([]map[string]string, 0)
	err := a.db.View(func(tx *bolt.Tx) error {
		availableRegistry := AvailableRegistry{
			Tx: tx,
		}
		return availableRegistry.ForEach(func(versionAvailable avro.ApplicationVersionAvailable) error {
			list = append(list, map[string]string{
				"version": versionAvailable.Version,
				"app":     versionAvailable.App,
			})
			return nil
		})
	})
	if err != nil {
		glog.Warningf("read versions failed: %v", err)
		http.Error(resp, "read versions failed", http.StatusInternalServerError)
	}
	resp.WriteHeader(http.StatusOK)
	resp.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(resp).Encode(list); err != nil {
		glog.Warningf("encode json failed: %v", err)
	}
}
