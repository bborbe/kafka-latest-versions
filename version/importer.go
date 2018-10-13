// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"context"

	"github.com/bborbe/kafka-latest-versions/avro"
)

type Importer struct {
	VersionUpdates <-chan avro.Version
	Store          interface {
		AddVersion(new avro.Version)
	}
}

func (i *Importer) Import(ctx context.Context) error {
	for {
		select {
		case new := <-i.VersionUpdates:
			i.Store.AddVersion(new)
		case <-ctx.Done():
			return nil
		}
	}
}
