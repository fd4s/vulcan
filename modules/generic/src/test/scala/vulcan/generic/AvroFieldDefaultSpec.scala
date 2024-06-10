/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import examples.AvroRecordDefault._
import org.apache.avro.JsonProperties

final class AvroFieldDefaultSpec extends CodecBase {
  describe("AvroFieldDefault") {
    it("should create a schema with a default for a field") {
      assert(Foo.codec.schema.exists(_.getField("a").defaultVal() == 1))
      assert(Foo.codec.schema.exists(_.getField("b").defaultVal() == "foo"))
      assert(Foo.codec.schema.exists(_.getField("c").defaultVal() == JsonProperties.NULL_VALUE))
    }

    it("should fail when annotating an Option") {
      assertSchemaError[InvalidDefault2]
    }

    it("should succeed when annotating an enum first element") {
      assert(HasSFirst.codec.schema.exists(_.getField("s").defaultVal() == "A"))
    }

    it("should succeed when annotating an enum second element") {
      assert(HasSSecond.codec.schema.exists(_.getField("s").defaultVal() == "B"))
    }

    it("should succeed with the first member of a union") {
      assertSchemaIs[HasUnion](
        """{"type":"record","name":"HasUnion","namespace":"vulcan.generic.examples.AvroRecordDefault","fields":[{"name":"u","type":[{"type":"record","name":"A","namespace":"vulcan.generic.examples.AvroRecordDefault.Union","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"B","namespace":"vulcan.generic.examples.AvroRecordDefault.Union","fields":[{"name":"b","type":"string"}]}],"default":{"a":1}}]}"""
      )
      val result = unsafeDecode[HasUnion](unsafeEncode[Empty](Empty()))
      assert(result == HasUnion(Union.A(1)))
    }

    it("should fail with the second member of a union") {
      assertSchemaError[HasUnionSecond]
    }
  }
}
