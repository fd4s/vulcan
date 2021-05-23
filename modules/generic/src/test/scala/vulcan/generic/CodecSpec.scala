package vulcan.generic

import cats.implicits._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan._
import vulcan.generic.examples._
import vulcan.internal.converters.collection._

final class CodecSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("Codec") {

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
              """{"type":"record","name":"CaseClassField","namespace":"vulcan.generic.examples","fields":[{"name":"value","type":"int"}]}"""
            }
          }

          it("should support annotation for record field documentation") {
            assertSchemaIs[CaseClassFieldAvroDoc] {
              """{"type":"record","name":"CaseClassFieldAvroDoc","namespace":"vulcan.generic.examples","fields":[{"name":"name","type":"string","doc":"documentation"}]}"""
            }
          }

          it("should support annotation for record documentation") {
            assertSchemaIs[CaseClassAvroDoc] {
              """{"type":"record","name":"CaseClassAvroDoc","namespace":"vulcan.generic.examples","doc":"documentation","fields":[{"name":"value","type":["null","string"]}]}"""
            }
          }

          it("should capture errors on invalid names") {
            assertSchemaError[CaseClassFieldInvalidName] {
              """org.apache.avro.SchemaParseException: Illegal initial character: -value"""
            }
          }

          it(
            "should support annotation for setting explicit default null values for all nullable fields"
          ) {
            assertSchemaIs[CaseClassAvroNullDefault] {
              """{"type":"record","name":"CaseClassAvroNullDefault","namespace":"vulcan.generic.examples","fields":[{"name":"int","type":["null","int"],"default":null},{"name":"long","type":["null","long"],"default":null},{"name":"string","type":["null","string"],"default":null},{"name":"date","type":["null",{"type":"int","logicalType":"date"}],"default":null},{"name":"map","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"caseClassValueClass","type":["null","int"],"default":null},{"name":"sealedTraitEnumDerived","type":["null",{"type":"enum","name":"SealedTraitEnumDerived","namespace":"com.example","symbols":["first","second"]}],"default":null}]}"""
            }
          }

          it("should support annotation for setting explicit default null value for specific field") {
            assertSchemaIs[CaseClassFieldAvroNullDefault] {
              """{"type":"record","name":"CaseClassFieldAvroNullDefault","namespace":"vulcan.generic.examples","fields":[{"name":"int","type":["null","int"],"default":null},{"name":"long","type":["null","long"]},{"name":"string","type":["null","string"],"default":null},{"name":"date","type":["null",{"type":"int","logicalType":"date"}]},{"name":"map","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"caseClassValueClass","type":["null","int"]},{"name":"sealedTraitEnumDerived","type":["null",{"type":"enum","name":"SealedTraitEnumDerived","namespace":"com.example","symbols":["first","second"]}],"default":null}]}"""
            }
          }

          it(
            "should support parameter annotation overriding case class annotation when setting explicit default null value for specific field"
          ) {
            assertSchemaIs[CaseClassAndFieldAvroNullDefault] {
              """{"type":"record","name":"CaseClassAndFieldAvroNullDefault","namespace":"vulcan.generic.examples","fields":[{"name":"int","type":["null","int"]},{"name":"long","type":["null","long"],"default":null},{"name":"string","type":["null","string"]},{"name":"date","type":["null",{"type":"int","logicalType":"date"}],"default":null},{"name":"map","type":["null",{"type":"map","values":"string"}]},{"name":"caseClassValueClass","type":["null","int"],"default":null},{"name":"sealedTraitEnumDerived","type":["null",{"type":"enum","name":"SealedTraitEnumDerived","namespace":"com.example","symbols":["first","second"]}]}]}"""
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
              "Error decoding vulcan.generic.examples.CaseClassField: Got unexpected schema type STRING, expected schema type RECORD"
            )
          }

          it("should error if value is not indexed record") {
            assertDecodeError[CaseClassField](
              unsafeEncode(123),
              unsafeSchema[CaseClassField],
              "Error decoding vulcan.generic.examples.CaseClassField: Got unexpected type java.lang.Integer, expected type IndexedRecord"
            )
          }

          it("should error if any field is missing in writer schema") {
            assertDecodeError[CaseClassField](
              {
                val schema =
                  Schema.createRecord("CaseClassField", null, "vulcan.generic.examples", false)

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
              "Error decoding vulcan.generic.examples.CaseClassField: Record writer schema is missing field 'value'"
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
              """[{"type":"record","name":"CaseClassInSealedTrait","namespace":"vulcan.generic.examples","fields":[{"name":"value","type":"int"}]}]"""
            }
          }

          it("should encode case objects as empty records") {
            assertSchemaIs[SealedTraitCaseObject] {
              """[{"type":"record","name":"CaseObjectInSealedTrait","namespace":"vulcan.generic.examples","fields":[]}]"""
            }
          }

          it("should capture errors on nested unions") {
            assertSchemaError[SealedTraitNestedUnion] {
              """org.apache.avro.AvroRuntimeException: Nested union: [["null","int"]]"""
            }
          }
        }

        describe("encode") {
          it("should error if value is not an alternative") {
            assertEncodeError[SealedTraitCaseClassIncomplete](
              SecondInSealedTraitCaseClassIncomplete(0),
              "Error encoding union: Exhausted alternatives for type vulcan.generic.examples.SecondInSealedTraitCaseClassIncomplete"
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
          it("should error if schema is not in union") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseClass](CaseClassInSealedTrait(0)),
              unsafeSchema[String],
              "Error decoding union: Missing schema CaseClassInSealedTrait in union"
            )
          }

          it("should decode if schema is part of union") {
            assertDecodeIs[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseClass](CaseClassInSealedTrait(0)),
              Right(CaseClassInSealedTrait(0)),
              Some(unsafeSchema[CaseClassInSealedTrait])
            )
          }

          it("should error if value is not an alternative") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode(123),
              unsafeSchema[SealedTraitCaseClass],
              "Error decoding union: Exhausted alternatives for type java.lang.Integer"
            )
          }

          it("should error if no schema in union with container name") {
            assertDecodeError[SealedTraitCaseObject](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseClass],
              "Error decoding union: Missing schema CaseObjectInSealedTrait in union"
            )
          }

          it("should error if no subtype with container name") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseObject],
              "Error decoding union: Missing alternative CaseObjectInSealedTrait in union"
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
