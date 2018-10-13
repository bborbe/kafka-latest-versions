// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version

import (
	"bytes"
	"context"

	"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/pkg/errors"
	"github.com/seibert-media/go-kafka/schema"
)

type MessageHandler struct {
	VersionUpdates chan<- avro.Version
}

func (b *MessageHandler) ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	value := bytes.NewBuffer(msg.Value)
	err := schema.RemoveMagicHeader(value)
	if err != nil {
		return errors.Wrap(err, "remove magic headers failed")
	}
	version, err := avro.DeserializeVersion(value)
	if err != nil {
		return errors.Wrap(err, "deserialize version failed")
	}
	b.VersionUpdates <- *version
	return nil
}
