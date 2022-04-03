package vulcan.generic

import cats.syntax.all._
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct}
import vulcan._
import vulcan.generic.examples._

final class CoproductCodecSpec extends CodecBase {
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
            """org.apache.avro.AvroRuntimeException: Nested union: ["int",["null","string"]]"""
          }
        }

        it("should fail if CNil schema is not union") {
          val result = Either.catchNonFatal {
            coproductCodec[Int, CNil](
              Codec.int,
              shapeless.Lazy {
                Codec.unit.asInstanceOf[Codec.Aux[Null, CNil]]
              }
            )
          }

          assert(result.swap.value.isInstanceOf[IllegalArgumentException])

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
}
