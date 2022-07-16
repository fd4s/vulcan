package vulcan.generic

import org.apache.avro.{Schema}
import org.apache.avro.generic.GenericData
import vulcan.generic.examples._
import vulcan.internal.converters.collection._

final class GenericDerivationCodecSpec extends CodecBase {
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

          it("should support annotation for record name") {
            assertSchemaIs[CaseClassAvroName] {
              """{"type":"record","name":"CaseClassOtherName","namespace":"vulcan.generic.examples","fields":[{"name":"value","type":["null","string"]}]}"""
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

          it("should support generic case classes and include the type name in the schema name (Int)") {
            assertSchemaIs[CaseClassTypeParameterField[Int]] {
              """{"type":"record","name":"CaseClassTypeParameterField__Int","namespace":"vulcan.generic.examples","fields":[{"name":"s","type":"string"},{"name":"value","type":"int"}]}"""
            }
          }

          it("should support generic case classes and include the type name in the schema name (Long)") {
            assertSchemaIs[CaseClassTypeParameterField[Long]] {
              """{"type":"record","name":"CaseClassTypeParameterField__Long","namespace":"vulcan.generic.examples","fields":[{"name":"s","type":"string"},{"name":"value","type":"long"}]}"""
            }
          }

          it("should support case classes with nested generic case classes and include the types in the schema name") {
            assertSchemaIs[CaseClassTypeParameterField[CaseClassInner[Int, Long]]] {
              """{"type":"record","name":"CaseClassTypeParameterField__CaseClassInner__Int_Long","namespace":"vulcan.generic.examples","fields":[{"name":"s","type":"string"},{"name":"value","type":{"type":"record","name":"CaseClassInner__Int_Long","fields":[{"name":"inner1","type":"int"},{"name":"inner2","type":"long"}]}}]}"""
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

          // Preserving existing behaviour from Magnolia for scala 2
          it("should order alternatives alphabetically by class name") {
            assertSchemaIs[SealedTraitCaseClassAvroNamespace](
              """[{"type":"record","name":"FirstInSealedTraitCaseClassAvroNamespace","namespace":"vulcan.generic.examples","fields":[{"name":"value","type":"int"}]},{"type":"record","name":"SecondInSealedTraitCaseClassAvroNamespace","namespace":"com.example","fields":[{"name":"value","type":"string"}]}]"""
            )
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
              "Error decoding vulcan.generic.examples.SealedTraitCaseClass: Error decoding union: Missing schema CaseClassInSealedTrait in union"
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
              "Error decoding vulcan.generic.examples.SealedTraitCaseClass: Error decoding union: Exhausted alternatives for type java.lang.Integer"
            )
          }

          it("should error if no schema in union with container name") {
            assertDecodeError[SealedTraitCaseObject](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseClass],
              "Error decoding vulcan.generic.examples.SealedTraitCaseObject: Error decoding union: Missing schema CaseObjectInSealedTrait in union"
            )
          }

          it("should error if no subtype with container name") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode[SealedTraitCaseObject](CaseObjectInSealedTrait),
              unsafeSchema[SealedTraitCaseObject],
              "Error decoding vulcan.generic.examples.SealedTraitCaseClass: Error decoding union: Missing alternative CaseObjectInSealedTrait in union"
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
}
