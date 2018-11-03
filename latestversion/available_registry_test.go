// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package latestversion_test

import (
	"io/ioutil"
	"os"

	"github.com/bborbe/kafka-latest-versions/avro"
	"github.com/bborbe/kafka-latest-versions/latestversion"
	"github.com/boltdb/bolt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AvailableRegistry", func() {
	var filename string
	var db *bolt.DB
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
	})
	AfterEach(func() {
		_ = os.Remove(filename)
	})
	It("return error if not exits", func() {
		err := db.View(func(tx *bolt.Tx) error {
			o := latestversion.AvailableRegistry{
				Tx: tx,
			}
			_, err := o.Get("foo")
			Expect(err).NotTo(BeNil())
			return nil
		})
		Expect(err).To(BeNil())
	})
	It("saves version", func() {
		err := db.Update(func(tx *bolt.Tx) error {
			versionRegistry := latestversion.AvailableRegistry{Tx: tx}
			err := versionRegistry.Set(avro.ApplicationVersionAvailable{App: "world", Version: "1.2.3"})
			Expect(err).To(BeNil())
			return nil
		})
		Expect(err).To(BeNil())
		err = db.View(func(tx *bolt.Tx) error {
			versionRegistry := latestversion.AvailableRegistry{Tx: tx}
			version, err := versionRegistry.Get("world")
			Expect(err).To(BeNil())
			Expect(version.App).To(Equal("world"))
			Expect(version.Version).To(Equal("1.2.3"))
			return nil
		})
		Expect(err).To(BeNil())
	})
})
