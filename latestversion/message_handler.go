// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"bytes"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/version"
	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/seibert-media/go-kafka/schema"
)

type MessageHandler struct {
	LatestVersionPublisher interface {
		Publish(avro.ApplicationVersionAvailable) error
	}
}

func (m *MessageHandler) HandleMessage(tx *bolt.Tx, msg *sarama.ConsumerMessage) error {
	buf := bytes.NewBuffer(msg.Value)
	if err := schema.RemoveMagicHeader(buf); err != nil {
		return errors.Wrap(err, "remove magic headers failed")
	}
	newVersion, err := avro.DeserializeApplicationVersionAvailable(buf)
	if err != nil {
		return errors.Wrap(err, "deserialize version failed")
	}
	if strings.Contains(newVersion.Version, "alpha") || strings.Contains(newVersion.Version, "beta") {
		glog.V(3).Infof("skip version %s because alpha or beta", newVersion.Version)
		return nil
	}
	versionRegistry := AvailableRegistry{
		Tx: tx,
	}
	currentVersion, _ := versionRegistry.Get(newVersion.App)
	if currentVersion == nil || version.Version(currentVersion.Version).Less(version.Version(newVersion.Version)) {
		if err := versionRegistry.Set(*newVersion); err != nil {
			return errors.Wrap(err, "save version failed")
		}
		if err := m.LatestVersionPublisher.Publish(*newVersion); err != nil {
			return errors.Wrap(err, "publish latest version failed")
		}
	}
	return nil
}