// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version_test

import (
	"testing"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/kafka-latest-versions/version"
)

func TestStore(t *testing.T) {
	versionStore := &version.Store{}
	if len(versionStore.Latest()) != 0 {
		t.Fatal("len should be 0")
	}

	versionStore.AddVersion(avro.Version{
		App: "world", Number: "1.0.0",
	})
	if len(versionStore.Latest()) != 1 {
		t.Fatal("len should be 1")
	}
	if versionStore.Latest()[0].Number != "1.0.0" {
		t.Fatal("should be versin 1.0.0")
	}

	versionStore.AddVersion(avro.Version{
		App: "world", Number: "0.9.0",
	})
	if len(versionStore.Latest()) != 1 {
		t.Fatal("len should be 1")
	}
	if versionStore.Latest()[0].Number != "1.0.0" {
		t.Fatal("should be versin 1.0.0")
	}

	versionStore.AddVersion(avro.Version{
		App: "world", Number: "1.1.0",
	})
	if len(versionStore.Latest()) != 1 {
		t.Fatal("len should be 1")
	}
	if versionStore.Latest()[0].Number != "1.1.0" {
		t.Fatal("should be versin 1.1.0")
	}

	versionStore.AddVersion(avro.Version{
		App: "second", Number: "1.1.0",
	})
	if len(versionStore.Latest()) != 2 {
		t.Fatal("len should be 2")
	}
}
