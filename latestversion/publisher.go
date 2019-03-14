// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion

import (
	"bytes"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/seibert-media/go-kafka/schema"
)

type Publisher struct {
	Producer       sarama.SyncProducer
	KafkaTopic     string
	SchemaRegistry schema.Registry
}

func (p *Publisher) Publish(version avro.ApplicationVersionAvailable) error {

	schemaId, err := p.SchemaRegistry.SchemaId(fmt.Sprintf("%s-value", p.KafkaTopic), version.Schema())
	if err != nil {
		return errors.Wrap(err, "get schema id failed")
	}
	buf := &bytes.Buffer{}
	if err := version.Serialize(buf); err != nil {
		return errors.Wrap(err, "serialize version failed")
	}
	partition, offset, err := p.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.KafkaTopic,
		Key:   sarama.StringEncoder(version.App),
		Value: &schema.AvroEncoder{SchemaId: schemaId, Content: buf.Bytes()},
	})
	if err != nil {
		return errors.Wrap(err, "send message to kafka failed")
	}
	glog.V(3).Infof("send message successful to %s with partition %d offset %d", p.KafkaTopic, partition, offset)
	return nil
}
