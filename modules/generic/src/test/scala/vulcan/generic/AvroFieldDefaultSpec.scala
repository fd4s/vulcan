/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.{AvroError, Codec}

final class AvroFieldDefaultSpec extends CodecBase {

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

  describe("AvroFieldDefault") {
    it("should create a schema with a default for a field") {
      case class Foo(
        @AvroFieldDefault(1) a: Int,
        @AvroFieldDefault("foo") b: String,
      )

      object Foo {
        implicit val codec: Codec[Foo] = Codec.derive
      }

      assert(Foo.codec.schema.exists(_.getField("a").defaultVal() == 1))
      assert(Foo.codec.schema.exists(_.getField("b").defaultVal() == "foo"))
    }

    it("should fail when the default value is not of the correct type") {
      case class InvalidDefault(
                                 @AvroFieldDefault("foo") a: Int
                               )
      object InvalidDefault {
        implicit val codec: Codec[InvalidDefault] = Codec.derive
      }

      assertSchemaError[InvalidDefault]
    }

    it("should fail when annotating an Option") {
      case class InvalidDefault2(
        @AvroFieldDefault(Some("foo")) a: Option[String]
      )
      object InvalidDefault2 {
        implicit val codec: Codec[InvalidDefault2] = Codec.derive
      }

      assertSchemaError[InvalidDefault2]
    }

    it("should succeed when annotating an enum first element") {
      case class HasSFirst(
                            @AvroFieldDefault(Enum.A) s: Enum
                          )
      object HasSFirst {
        implicit val codec: Codec[HasSFirst] = Codec.derive
      }

      assert(HasSFirst.codec.schema.exists(_.getField("s").defaultVal() == "A"))
    }

    it("should succeed when annotating an enum second element") {
      case class HasSSecond(
         @AvroFieldDefault(Enum.B) s: Enum
       )
      object HasSSecond {
        implicit val codec: Codec[HasSSecond] = Codec.derive
      }

      assert(HasSSecond.codec.schema.exists(_.getField("s").defaultVal() == "B"))
    }

    it("should succeed with the first member of a union"){
      case class HasUnion(
        @AvroFieldDefault(Union.A(1)) u: Union
      )
      object HasUnion {
        implicit val codec: Codec[HasUnion] = Codec.derive
      }

      case class Empty()
      object Empty {
        implicit val codec: Codec[Empty] = Codec.derive
      }

      assertSchemaIs[HasUnion](
        """{"type":"record","name":"HasUnion","namespace":"vulcan.generic.AvroFieldDefaultSpec.<local AvroFieldDefaultSpec>","fields":[{"name":"u","type":[{"type":"record","name":"A","namespace":"vulcan.generic.AvroFieldDefaultSpec.Union","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"B","namespace":"vulcan.generic.AvroFieldDefaultSpec.Union","fields":[{"name":"b","type":"string"}]}],"default":{"a":1}}]}"""
      )

      val result = unsafeDecode[HasUnion](unsafeEncode[Empty](Empty()))

      assert(result == HasUnion(Union.A(1)))

    }

    it("should fail with the second member of a union"){
      case class HasUnionSecond(
        @AvroFieldDefault(Union.B("foo")) u: Union
      )
      object HasUnionSecond {
        implicit val codec: Codec[HasUnionSecond] = Codec.derive
      }

      assertSchemaError[HasUnionSecond]
    }
  }
}
