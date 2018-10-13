// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"context"
	"sync"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/version"
)

type Store struct {
	VersionUpdates <-chan avro.Version

	mux            sync.Mutex
	latestVersions map[string]avro.Version
}

func (s *Store) Import(ctx context.Context) error {
	s.mux.Lock()
	if s.latestVersions == nil {
		s.latestVersions = make(map[string]avro.Version)
	}
	s.mux.Unlock()
	for {
		select {
		case new := <-s.VersionUpdates:
			s.importVersion(new)
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Store) importVersion(new avro.Version) {
	s.mux.Lock()
	defer s.mux.Unlock()
	current, found := s.latestVersions[new.App]
	if !found {
		s.latestVersions[new.App] = new
		return
	}
	if version.GreaterThan(version.Version(new.Number), version.Version(current.Number)) {
		s.latestVersions[new.App] = new
	}
}

func (s *Store) Latest() []avro.Version {
	var result []avro.Version
	s.mux.Lock()
	defer s.mux.Unlock()
	for _, v := range s.latestVersions {
		result = append(result, v)
	}
	return result
}