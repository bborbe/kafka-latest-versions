// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"bytes"
	"fmt"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

type AvailableRegistry struct {
	Tx *bolt.Tx
}

func (a *AvailableRegistry) Get(app string) (*avro.ApplicationVersionAvailable, error) {
	bucket := a.Tx.Bucket([]byte("version"))
	buf := bytes.NewBuffer(bucket.Get([]byte(app)))
	if buf == nil {
		return nil, fmt.Errorf("not found")
	}
	version, err := avro.DeserializeApplicationVersionAvailable(buf)
	return version, errors.Wrap(err, "deserialize version failed")
}

func (a *AvailableRegistry) Set(version avro.ApplicationVersionAvailable) error {
	key := []byte(version.App)
	if len(key) == 0 {
		return errors.New("key empty")
	}
	bucket := a.Tx.Bucket([]byte("version"))
	buf := &bytes.Buffer{}
	if err := version.Serialize(buf); err != nil {
		return errors.Wrap(err, "serialize version failed")
	}
	err := bucket.Put(key, buf.Bytes())
	return errors.Wrap(err, "put version failed")
}

func (a *AvailableRegistry) ForEach(fn func(avro.ApplicationVersionAvailable) error) error {
	bucket := a.Tx.Bucket([]byte("version"))
	return bucket.ForEach(func(k, v []byte) error {
		version, err := avro.DeserializeApplicationVersionAvailable(bytes.NewBuffer(v))
		if err != nil {
			return errors.Wrap(err, "deserialize version failed")
		}
		return fn(*version)
	})
}
