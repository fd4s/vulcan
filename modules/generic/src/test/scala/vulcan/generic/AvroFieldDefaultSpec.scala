/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.Codec

case class Foo(
  a: Int = 1,
  b: String = "foo",
)

object Foo {
  implicit val codec: Codec[Foo] = Codec.derive
}

final class AvroFieldDefaultSpec extends CodecBase {
  describe("AvroFieldDefault") {
    it("should create a schema with a default for a field") {

      assert(Foo.codec.schema.exists(_.getField("a").defaultVal() == 1))
      assert(Foo.codec.schema.exists(_.getField("b").defaultVal() == "foo"))
    }
  }
}