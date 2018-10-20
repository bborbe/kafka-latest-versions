// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"bytes"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/version"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/seibert-media/go-kafka/schema"
)

type OffsetMessageHandler struct {
	DB *bolt.DB
}

func (f *OffsetMessageHandler) NextOffset(partition int32) (int64, error) {
	offset := sarama.OffsetOldest
	f.DB.View(func(tx *bolt.Tx) error {
		offsetRegistry := OffsetRegistry{
			Tx: tx,
		}
		value, err := offsetRegistry.Get(partition)
		if err != nil {
			return err
		}
		offset = value
		return nil
	})
	return offset, nil
}

func (f *OffsetMessageHandler) HandleMessage(partition int32, msg *sarama.ConsumerMessage) error {
	return f.DB.Update(func(tx *bolt.Tx) error {
		offsetRegistry := OffsetRegistry{
			Tx: tx,
		}
		versionRegistry := VersionRegistry{
			Tx: tx,
		}
		if err := offsetRegistry.Set(partition, msg.Offset+1); err != nil {
			return errors.Wrap(err, "set offest failed")
		}
		buf := bytes.NewBuffer(msg.Value)
		if err := schema.RemoveMagicHeader(buf); err != nil {
			return errors.Wrap(err, "remove magic headers failed")
		}
		new, err := avro.DeserializeApplicationVersionAvailable(buf)
		if err != nil {
			return errors.Wrap(err, "deserialize version failed")
		}
		if strings.Contains(new.Version, "alpha") || strings.Contains(new.Version, "beta") {
			return nil
		}
		current, _ := versionRegistry.Get(new.App)
		if current == nil || version.Version(current.Version).Less(version.Version(new.Version)) {
			err := versionRegistry.Set(*new)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
