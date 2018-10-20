// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version_test

import (
	"bytes"
	"github.com/Shopify/sarama" //"github.com/Shopify/sarama"
	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/kafka-latest-versions/version"
	"github.com/boltdb/bolt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
)

var _ = Describe("OffsetMessageHandler", func() {
	var filename string
	var db *bolt.DB
	BeforeEach(func() {
		file, err := ioutil.TempFile("", "")
		Expect(err).To(BeNil())
		filename = file.Name()
		db, err = bolt.Open(filename, 0600, nil)
		Expect(err).To(BeNil())

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("offset"))
			tx.CreateBucket([]byte("version"))
			return nil
		})

	})
	AfterEach(func() {
		os.Remove(filename)
	})
	It("return -2 if next offset not found", func() {
		offsetMessageHandler := version.OffsetMessageHandler{
			DB: db,
		}
		offset, err := offsetMessageHandler.NextOffset(0)
		Expect(err).To(BeNil())
		Expect(offset).To(Equal(int64(-2)))
	})
	It("handles message", func() {
		v := &avro.ApplicationVersionAvailable{
			App:    "world",
			Version: "1.2.3",
		}
		buf := bytes.NewBuffer(make([]byte, 5))
		err := v.Serialize(buf)
		Expect(err).To(BeNil())
		offsetMessageHandler := version.OffsetMessageHandler{
			DB: db,
		}
		err = offsetMessageHandler.HandleMessage(0, &sarama.ConsumerMessage{
			Offset: 1000,
			Value:  buf.Bytes(),
		})
		Expect(err).To(BeNil())

		nextOffset, err := offsetMessageHandler.NextOffset(0)
		Expect(err).To(BeNil())
		Expect(nextOffset).To(Equal(int64(1001)))

		db.View(func(tx *bolt.Tx) error {
			versionRegistry := version.VersionRegistry{Tx: tx}
			version, err := versionRegistry.Get("world")
			Expect(err).To(BeNil())
			Expect(version.App).To(Equal("world"))
			Expect(version.Version).To(Equal("1.2.3"))
			return nil
		})
	})
})
