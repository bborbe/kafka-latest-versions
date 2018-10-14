// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"bytes"
	"fmt"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

type VersionRegistry struct {
	Tx *bolt.Tx
}

func (v *VersionRegistry) Get(app string) (*avro.Version, error) {
	bucket := v.Tx.Bucket([]byte("version"))
	buf := bytes.NewBuffer(bucket.Get([]byte(app)))
	if buf == nil {
		return nil, fmt.Errorf("not found")
	}
	version, err := avro.DeserializeVersion(buf)
	return version, errors.Wrap(err, "deserialize version failed")
}

func (v *VersionRegistry) Set(version avro.Version) error {
	bucket := v.Tx.Bucket([]byte("version"))
	buf := &bytes.Buffer{}
	if err := version.Serialize(buf); err != nil {
		return errors.Wrap(err, "serialize version failed")
	}
	err := bucket.Put([]byte(version.App), buf.Bytes())
	return errors.Wrap(err, "put version failed")
}

func (v *VersionRegistry) ForEach(fn func(avro.Version) error) error {
	bucket := v.Tx.Bucket([]byte("version"))
	return bucket.ForEach(func(k, v []byte) error {
		version, err := avro.DeserializeVersion(bytes.NewBuffer(v))
		if err != nil {
			return errors.Wrap(err, "deserialize version failed")
		}
		return fn(*version)
	})
}
