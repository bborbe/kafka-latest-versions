// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"strings"
	"sync"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/version"
)

type Store struct {
	mux            sync.Mutex
	latestVersions map[string]avro.Version
}

func (s *Store) AddVersion(new avro.Version) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.latestVersions == nil {
		s.latestVersions = make(map[string]avro.Version)
	}

	if strings.Contains(new.Number, "alpha") || strings.Contains(new.Number, "beta") {
		return
	}

	current, found := s.latestVersions[new.App]
	if !found {
		s.latestVersions[new.App] = new
		return
	}
	if version.Version(current.Number).Less(version.Version(new.Number)) {
		s.latestVersions[new.App] = new
	}
}

func (s *Store) Latest() []avro.Version {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.latestVersions == nil {
		s.latestVersions = make(map[string]avro.Version)
	}

	var result []avro.Version
	for _, v := range s.latestVersions {
		result = append(result, v)
	}
	return result
}
