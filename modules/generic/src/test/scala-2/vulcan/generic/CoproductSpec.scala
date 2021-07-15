package vulcan.generic

import cats.implicits._
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import shapeless.{:+:, CNil, Coproduct}
import vulcan._
import vulcan.generic.examples._

final class CoproductSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("Codec") {
    describe("cnil") {
      describe("schema") {
        it("should be encoded as empty union") {
          assertSchemaIs[CNil] {
            """[]"""
          }
        }
      }

      describe("encode") {
        it("should error") {
          assertEncodeError[CNil](
            null,
            "Error encoding Coproduct: Exhausted alternatives for type null"
          )
        }
      }

      describe("decode") {
        it("should error") {
          assertDecodeError[CNil](
            null,
            unsafeSchema[CNil],
            "Error decoding Coproduct: Exhausted alternatives for type null"
          )
        }
      }
    }

    describe("coproduct") {
      describe("schema") {
        it("should be encoded as union") {
          assertSchemaIs[Int :+: String :+: CNil] {
            """["int","string"]"""
          }
        }

        it("should capture errors on nested unions") {
          assertSchemaError[Int :+: Option[String] :+: CNil] {
            """org.apache.avro.AvroRuntimeException: Nested union: [["null","string"]]"""
          }
        }

        it("should fail if CNil schema is not union") {
          val codec: Codec[Int :+: CNil] =
            coproductCodec[Int, CNil](
              Codec.int,
              shapeless.Lazy {
                Codec.instance[Null, CNil](
                  Right(SchemaBuilder.builder().nullType()),
                  _ => Left(AvroError("encode")),
                  (_, _) => Left(AvroError("decode"))
                )
              }
            )

          assertSchemaError[Int :+: CNil] {
            """Unexpected schema type NULL in Coproduct"""
          }(codec)
        }
      }

      describe("encode") {
        it("should encode first in coproduct using first type") {
          type A = Int :+: String :+: CNil
          assertEncodeIs[A](
            Coproduct[A](123),
            Right(unsafeEncode(123))
          )
        }

        it("should encode second in coproduct using second type") {
          type A = Int :+: String :+: CNil
          assertEncodeIs[A](
            Coproduct[A]("abc"),
            Right(unsafeEncode("abc"))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not in union") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](123)),
            unsafeSchema[String],
            "Error decoding Coproduct: Exhausted alternatives for type java.lang.Integer"
          )
        }

        it("should decode if schema is part of union") {
          type A = Int :+: String :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](123)),
            Right(Coproduct[A](123)),
            Some(unsafeSchema[Int])
          )
        }

        it("should error on empty union schema") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](123)),
            Schema.createUnion(),
            "Error decoding Coproduct: Exhausted alternatives for type java.lang.Integer"
          )
        }

        it("should decode first in coproduct using first type") {
          type A = Int :+: String :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](123)),
            Right(Coproduct[A](123))
          )
        }

        it("should decode second in coproduct using second type") {
          type A = Int :+: String :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A]("abc")),
            Right(Coproduct[A]("abc"))
          )
        }

        it("should decode coproduct with records") {
          type A = CaseClassField :+: CaseClassTwoFields :+: Int :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](CaseClassField(10))),
            Right(Coproduct[A](CaseClassField(10)))
          )

          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](CaseClassTwoFields("name", 10))),
            Right(Coproduct[A](CaseClassTwoFields("name", 10)))
          )

          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](123)),
            Right(Coproduct[A](123))
          )
        }

        it("should error if no schema in union with container name") {
          type A = Int :+: CaseClassField :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](CaseClassField(10))),
            unsafeSchema[Int :+: String :+: CNil],
            "Error decoding Coproduct: Missing schema CaseClassField in union"
          )
        }

        it("should error when not enough union schemas") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A]("abc")),
            Schema.createUnion(),
            "Error decoding Coproduct: Exhausted alternatives for type org.apache.avro.util.Utf8"
          )
        }

        it("should error when not enough union schemas when decoding record") {
          type A = Int :+: CaseClassField :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](CaseClassField(10))),
            unsafeSchema[CNil],
            "Error decoding Coproduct: Missing schema CaseClassField in union"
          )
        }
      }
    }
  }

  def unsafeSchema[A](implicit codec: Codec[A]): Schema =
    codec.schema.value

  def unsafeEncode[A](a: A)(implicit codec: Codec[A]): Any =
    codec.encode(a).value

  def unsafeDecode[A](value: Any)(implicit codec: Codec[A]): A =
    codec.schema.flatMap(codec.decode(value, _)).value

  def assertSchemaIs[A](expectedSchema: String)(implicit codec: Codec[A]): Assertion =
    assert(codec.schema.value.toString == expectedSchema)

  def assertEncodeIs[A](
    a: A,
    encoded: Either[AvroError, Any]
  )(implicit codec: Codec[A]): Assertion =
    assert(unsafeEncode(a) === encoded.value)

  def assertDecodeIs[A](
    value: Any,
    decoded: Either[AvroError, A],
    schema: Option[Schema] = None
  )(implicit codec: Codec[A]): Assertion =
    assert {
      val decode =
        schema
          .map(codec.decode(value, _).value)
          .getOrElse(unsafeDecode[A](value))

      decode === decoded.value
    }

  def assertSchemaError[A](
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.schema.swap.value.message == expectedErrorMessage)

  def assertDecodeError[A](
    value: Any,
    schema: Schema,
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.decode(value, schema).swap.value.message == expectedErrorMessage)

  def assertEncodeError[A](
    a: A,
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.encode(a).swap.value.message == expectedErrorMessage)
}
