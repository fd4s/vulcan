/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.{AvroError, Codec}


sealed trait Enum extends Product {
  self =>
  def value: String = self.productPrefix
}

object Enum {
  case object A extends Enum

  case object B extends Enum

  implicit val codec: Codec[Enum] = deriveEnum(
    symbols = List(A.value, B.value),
    encode = _.value,
    decode = {
      case "A" => Right(A)
      case "B" => Right(B)
      case other => Left(AvroError(s"Invalid S: $other"))
    }
  )
}

sealed trait Union

object Union {
  case class A(a: Int) extends Union

  case class B(b: String) extends Union

  implicit val codec: Codec[Union] = Codec.derive
}

case class Foo(
  a: Int = 1,
  b: String = "foo",
)

object Foo {
  implicit val codec: Codec[Foo] = Codec.derive
}

case class InvalidDefault2(
  a: Option[String] = Some("foo")
)
object InvalidDefault2 {
  implicit val codec: Codec[InvalidDefault2] = Codec.derive
}

case class HasSFirst(
  s: Enum = Enum.A
)
object HasSFirst {
  implicit val codec: Codec[HasSFirst] = Codec.derive
}

case class HasSSecond(
  s: Enum = Enum.B
)
object HasSSecond {
  implicit val codec: Codec[HasSSecond] = Codec.derive
}

case class HasUnion(
  u: Union = Union.A(1)
)
object HasUnion {
  implicit val codec: Codec[HasUnion] = Codec.derive
}

case class Empty()
object Empty {
  implicit val codec: Codec[Empty] = Codec.derive
}

case class HasUnionSecond(
  u: Union = Union.B("foo")
)
object HasUnionSecond {
  implicit val codec: Codec[HasUnionSecond] = Codec.derive
}

final class AvroFieldDefaultSpec extends CodecBase {
  describe("AvroFieldDefault") {
    it("should create a schema with a default for a field") {
      assert(Foo.codec.schema.exists(_.getField("a").defaultVal() == 1))
      assert(Foo.codec.schema.exists(_.getField("b").defaultVal() == "foo"))
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

    it("should succeed with the first member of a union"){
      assertSchemaIs[HasUnion](
        """{"type":"record","name":"HasUnion","namespace":"vulcan.generic","fields":[{"name":"u","type":[{"type":"record","name":"A","namespace":"vulcan.generic.Union","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"B","namespace":"vulcan.generic.Union","fields":[{"name":"b","type":"string"}]}],"default":{"a":1}}]}"""
      )
      val result = unsafeDecode[HasUnion](unsafeEncode[Empty](Empty()))
      assert(result == HasUnion(Union.A(1)))
    }

    it("should fail with the second member of a union"){
      assertSchemaError[HasUnionSecond]
    }
  }
}
