// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package version_test

import (
	"io/ioutil"
	"os"

	"github.com/bborbe/kafka-latest-versions/version"
	"github.com/boltdb/bolt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("OffsetRegistry", func() {
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
			return nil
		})

	})
	AfterEach(func() {
		os.Remove(filename)
	})
	It("return error if not exits", func() {
		db.View(func(tx *bolt.Tx) error {
			o := version.OffsetRegistry{
				Tx: tx,
			}
			_, err := o.Get(1)
			Expect(err).NotTo(BeNil())
			return nil
		})
	})
	It("saves offset", func() {
		offset := int64(42)
		db.Update(func(tx *bolt.Tx) error {
			offsetRegistry := version.OffsetRegistry{Tx: tx}
			offsetRegistry.Set(2, offset)
			return nil
		})
		db.View(func(tx *bolt.Tx) error {
			offsetRegistry := version.OffsetRegistry{Tx: tx}
			offset, err := offsetRegistry.Get(2)
			Expect(err).To(BeNil())
			Expect(offset).To(Equal(offset))
			return nil
		})
	})
})
