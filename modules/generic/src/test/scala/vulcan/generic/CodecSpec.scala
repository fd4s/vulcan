package vulcan.generic

import cats.implicits._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import shapeless.{:+:, CNil, Coproduct}
import vulcan._
import vulcan.examples._
import vulcan.internal.converters.collection._

final class CodecSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
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
            unsafeSchema[CNil],
            "CNil"
          )
        }
      }

      describe("decode") {
        it("should error") {
          assertDecodeError[CNil](
            null,
            unsafeSchema[CNil],
            "Unable to decode to any type in Coproduct"
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
                Codec.instance[CNil](
                  Right(SchemaBuilder.builder().nullType()),
                  (_, _) => Left(AvroError("encode")),
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
        it("should error if schema is not union") {
          type A = Int :+: String :+: CNil
          assertEncodeError[A](
            Coproduct[A](123),
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Coproduct, expected schema type UNION"
          )
        }

        it("should error on empty union schema") {
          type A = Int :+: String :+: CNil
          assertEncodeError[A](
            Coproduct[A](123),
            Schema.createUnion(),
            "Not enough types in union schema while encoding type Coproduct"
          )
        }

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
        it("should error if schema is not union") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](123)),
            unsafeSchema[String],
            "Got unexpected schema type STRING while decoding Coproduct, expected schema type UNION"
          )
        }

        it("should error on empty union schema") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](123)),
            Schema.createUnion(),
            "Not enough types in union schema while decoding type Coproduct"
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
            "Missing schema vulcan.examples.CaseClassField in union for type Coproduct"
          )
        }

        it("should error when not enough union schemas") {
          type A = Int :+: String :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A]("abc")),
            Schema.createUnion(),
            "Not enough types in union schema while decoding type Coproduct"
          )
        }

        it("should error when not enough union schemas when decoding record") {
          type A = Int :+: CaseClassField :+: CNil
          assertDecodeError[A](
            unsafeEncode(Coproduct[A](CaseClassField(10))),
            unsafeSchema[CNil],
            "Not enough types in union schema while decoding type Coproduct"
          )
        }
      }
    }

    describe("derive") {
      describe("caseClass") {
        describe("schema") {
          it("should encode value classes as underlying type") {
            assertSchemaIs[CaseClassValueClass] {
              """"int""""
            }
          }

          it("should use parameter schema for record field") {
            assertSchemaIs[CaseClassField] {
              """{"type":"record","name":"CaseClassField","namespace":"vulcan.examples","fields":[{"name":"value","type":"int"}]}"""
            }
          }

          it("should support annotation for record field documentation") {
            assertSchemaIs[CaseClassFieldAvroDoc] {
              """{"type":"record","name":"CaseClassFieldAvroDoc","namespace":"vulcan.examples","fields":[{"name":"name","type":"string","doc":"documentation"}]}"""
            }
          }

          it("should support annotation for record documentation") {
            assertSchemaIs[CaseClassAvroDoc] {
              """{"type":"record","name":"CaseClassAvroDoc","namespace":"vulcan.examples","doc":"documentation","fields":[{"name":"value","type":["null","string"]}]}"""
            }
          }

          it("should capture errors on invalid names") {
            assertSchemaError[CaseClassFieldInvalidName] {
              """org.apache.avro.SchemaParseException: Illegal initial character: -value"""
            }
          }
        }

        describe("encode") {
          it("should encode value class using encoder for underlying type") {
            assertEncodeIs[CaseClassValueClass](
              CaseClassValueClass(0),
              Right(unsafeEncode(0))
            )
          }

          it("should error if schema is not record") {
            assertEncodeError[CaseClassField](
              CaseClassField(0),
              unsafeSchema[Int],
              "Got unexpected schema type INT while encoding vulcan.examples.CaseClassField, expected schema type RECORD"
            )
          }

          it("should error if record schema name does not match") {
            assertEncodeError[CaseClassField](
              CaseClassField(0),
              unsafeSchema[CaseClassFieldAvroDoc],
              "Unable to encode vulcan.examples.CaseClassField using schema with name vulcan.examples.CaseClassFieldAvroDoc since names do not match"
            )
          }

          it("should error if record schema is missing any field") {
            assertEncodeError[CaseClassField](
              CaseClassField(0), {
                SchemaBuilder
                  .record("vulcan.examples.CaseClassField")
                  .fields()
                  .endRecord()
              },
              "Record field 'value' in schema is missing for type vulcan.examples.CaseClassField"
            )
          }

          it("should encode as record") {
            assertEncodeIs[CaseClassField](
              CaseClassField(0),
              Right {
                val record = new GenericData.Record(unsafeSchema[CaseClassField])
                record.put(0, unsafeEncode(0))
                record
              }
            )
          }

          it("should encode as record with multiple fields") {
            assertEncodeIs[CaseClassTwoFields](
              CaseClassTwoFields("the-name", 0),
              Right {
                val record = new GenericData.Record(unsafeSchema[CaseClassTwoFields])
                record.put(0, unsafeEncode("the-name"))
                record.put(1, unsafeEncode(0))
                record
              }
            )
          }
        }

        describe("decode") {
          it("should decode value class using decoder for underlying type") {
            assertDecodeIs[CaseClassValueClass](
              unsafeEncode(123),
              Right(CaseClassValueClass(123))
            )
          }

          it("should error if schema is not record") {
            assertDecodeError[CaseClassField](
              unsafeEncode(CaseClassField(123)),
              unsafeSchema[String],
              "Got unexpected schema type STRING while decoding vulcan.examples.CaseClassField, expected schema type RECORD"
            )
          }

          it("should error if value is not indexed record") {
            assertDecodeError[CaseClassField](
              unsafeEncode(123),
              unsafeSchema[CaseClassField],
              "Got unexpected type java.lang.Integer while decoding vulcan.examples.CaseClassField, expected type IndexedRecord"
            )
          }

          it("should error if writer schema full name does not match") {
            assertDecodeError[CaseClassField](
              {
                val schema =
                  Schema.createRecord("Record", null, "com.xyz", false)

                schema.setFields(
                  List(
                    new Schema.Field(
                      "value",
                      unsafeSchema[Int],
                      null
                    )
                  ).asJava
                )

                val record = new GenericData.Record(schema)
                record.put(0, 123)
                record
              },
              unsafeSchema[CaseClassField],
              "Got record writer schema with name com.xyz.Record, expected name vulcan.examples.CaseClassField"
            )
          }

          it("should error if any field is missing in writer schema") {
            assertDecodeError[CaseClassField](
              {
                val schema =
                  Schema.createRecord("CaseClassField", null, "vulcan.examples", false)

                schema.setFields(
                  List(
                    new Schema.Field(
                      "other",
                      unsafeSchema[Int],
                      null
                    )
                  ).asJava
                )

                val record = new GenericData.Record(schema)
                record.put(0, 123)
                record
              },
              unsafeSchema[CaseClassField],
              "Record writer schema is missing field 'value' while decoding vulcan.examples.CaseClassField"
            )
          }

          it("should decode as case class") {
            assertDecodeIs[CaseClassField](
              unsafeEncode(CaseClassField(123)),
              Right(CaseClassField(123))
            )
          }
        }
      }

      describe("sealedTrait") {
        describe("schema") {
          it("should encode case classes as records") {
            assertSchemaIs[SealedTraitCaseClass] {
              """[{"type":"record","name":"CaseClassInSealedTrait","namespace":"vulcan.examples","fields":[{"name":"value","type":"int"}]}]"""
            }
          }

          it("should encode case objects as empty records") {
            assertSchemaIs[SealedTraitCaseObject] {
              """[{"type":"record","name":"CaseObjectInSealedTrait","namespace":"vulcan.examples","fields":[]}]"""
            }
          }

          it("should capture errors on nested unions") {
            assertSchemaError[SealedTraitNestedUnion] {
              """org.apache.avro.AvroRuntimeException: Nested union: [["null","int"]]"""
            }
          }
        }

        describe("encode") {
          it("should error if schema is not union") {
            assertEncodeError[SealedTraitCaseClass](
              CaseClassInSealedTrait(0),
              unsafeSchema[Int],
              "Got unexpected schema type INT while encoding vulcan.examples.SealedTraitCaseClass, expected schema type UNION"
            )
          }

          it("should error if subtype name is not in union") {
            assertEncodeError[SealedTraitCaseClass](
              CaseClassInSealedTrait(0),
              unsafeSchema[Option[Int]],
              "Missing schema vulcan.examples.CaseClassInSealedTrait in union for type vulcan.examples.SealedTraitCaseClass"
            )
          }

          it("should encode with encoder for subtype") {
            val value = CaseClassInSealedTrait(0)
            assertEncodeIs[SealedTraitCaseClass](
              value,
              Right(unsafeEncode[SealedTraitCaseClass](value))
            )
          }
        }

        describe("decode") {
          it("should error if schema is not union") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseClass](CaseClassInSealedTrait(0)),
              unsafeSchema[String],
              "Got unexpected schema type STRING while decoding vulcan.examples.SealedTraitCaseClass, expected schema type UNION"
            )
          }

          it("should error if value is not an alternative") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode(123),
              unsafeSchema[SealedTraitCaseClass],
              "Exhausted alternatives for type java.lang.Integer while decoding vulcan.examples.SealedTraitCaseClass"
            )
          }

          it("should error if no schema in union with container name") {
            assertDecodeError[SealedTraitCaseObject](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseClass],
              "Missing schema vulcan.examples.CaseObjectInSealedTrait in union for type vulcan.examples.SealedTraitCaseObject"
            )
          }

          it("should error if no subtype with container name") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseObject],
              "Missing alternative vulcan.examples.CaseObjectInSealedTrait in union for type vulcan.examples.SealedTraitCaseClass"
            )
          }

          it("should decode using schema and decoder for subtype") {
            assertDecodeIs[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseClass](CaseClassInSealedTrait(0)),
              Right(CaseClassInSealedTrait(0))
            )
          }
        }
      }
    }
  }

  def unsafeSchema[A](implicit codec: Codec[A]): Schema =
    codec.schema.value

  def unsafeEncode[A](a: A)(implicit codec: Codec[A]): Any =
    codec.schema.flatMap(codec.encode(a, _)).value

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
    decoded: Either[AvroError, A]
  )(implicit codec: Codec[A]): Assertion =
    assert(unsafeDecode[A](value) === decoded.value)

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
    schema: Schema,
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.encode(a, schema).swap.value.message == expectedErrorMessage)
}
