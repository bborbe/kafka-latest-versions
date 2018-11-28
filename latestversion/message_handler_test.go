// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion_test

import (
	"bytes"
	"io/ioutil"
	"os"

	"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/kafka-latest-versions/latestversion"
	"github.com/bborbe/kafka-latest-versions/mocks"
	"github.com/boltdb/bolt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MessageHandler", func() {
	var filename string
	var db *bolt.DB
	var versionPublisher *mocks.VersionPublisher
	var messageHandler *latestversion.MessageHandler

	BeforeEach(func() {
		file, err := ioutil.TempFile("", "")
		Expect(err).To(BeNil())
		filename = file.Name()
		db, err = bolt.Open(filename, 0600, nil)
		Expect(err).To(BeNil())
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("version"))
			return err
		})
		Expect(err).To(BeNil())
		versionPublisher = &mocks.VersionPublisher{}
		messageHandler = &latestversion.MessageHandler{
			LatestVersionPublisher: versionPublisher,
		}
	})
	AfterEach(func() {
		_ = os.Remove(filename)
	})
	It("call publish", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(1))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if beta", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0-beta"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if alpha", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0-alpha"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if alpha-number", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0-alpha.0"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if milestone", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0.m0025-eap08"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if eap", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "1.0.0-eap08"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("doesn't call publish if rc", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			err := messageHandler.HandleMessage(tx, buildMessage("Test", "v1.13.0-rc.1"))
			Expect(err).To(BeNil())
			Expect(versionPublisher.PublishCallCount()).To(Equal(0))
			return nil
		})
		Expect(err).To(BeNil())
	})
})

func buildMessage(app string, version string) *sarama.ConsumerMessage {
	versionAvailable := &avro.ApplicationVersionAvailable{
		App:     app,
		Version: version,
	}
	buf := bytes.NewBuffer(make([]byte, 5))
	err := versionAvailable.Serialize(buf)
	Expect(err).To(BeNil())
	return &sarama.ConsumerMessage{
		Value: sarama.ByteEncoder(buf.Bytes()),
	}
}
