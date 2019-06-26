package vulcan

import cats.data._
import cats.implicits._
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate}
import java.util.UUID
import org.apache.avro.{Conversions, Schema, SchemaBuilder, LogicalTypes}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.Assertion
import org.scalatest.EitherValues._
import scala.collection.JavaConverters._
import shapeless.{:+:, CNil, Coproduct}
import vulcan.examples._

final class CodecSpec extends BaseSpec {
  describe("Codec") {
    describe("boolean") {
      describe("schema") {
        it("should be encoded as boolean") {
          assertSchemaIs[Boolean] {
            """"boolean""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not boolean") {
          assertEncodeError[Boolean](
            false,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Boolean, expected schema type BOOLEAN"
          )
        }

        it("should encode as boolean") {
          val value = true
          assertEncodeIs[Boolean](
            value,
            Right(java.lang.Boolean.valueOf(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not boolean") {
          assertDecodeError[Boolean](
            unsafeEncode(false),
            unsafeSchema[Long],
            "Got unexpected schema type LONG while decoding Boolean, expected schema type BOOLEAN"
          )
        }

        it("should error if value is not boolean") {
          assertDecodeError[Boolean](
            unsafeEncode(10L),
            unsafeSchema[Boolean],
            "Got unexpected type java.lang.Long while decoding Boolean, expected type Boolean"
          )
        }

        it("should error if value is null") {
          assertDecodeError[Boolean](
            null,
            unsafeSchema[Boolean],
            "Got unexpected type null while decoding Boolean, expected type Boolean"
          )
        }

        it("should decode as Boolean") {
          val value = false
          assertDecodeIs[Boolean](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("bytes") {
      describe("schema") {
        it("should be encoded as bytes") {
          assertSchemaIs[Array[Byte]] {
            """"bytes""""
          }
        }
      }

      describe("encode") {
        it("should error on non-fixed, non-bytes schema") {
          assertEncodeError[Array[Byte]](
            Array(1),
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Array[Byte], expected schema types BYTES or FIXED"
          )
        }

        it("should encode as bytes") {
          assertEncodeIs[Array[Byte]](
            Array(1),
            Right(ByteBuffer.wrap(Array(1)))
          )
        }

        describe("fixed") {
          it("should error if bytes exceed schema size") {
            assertEncodeError[Array[Byte]](
              Array(1, 2, 3),
              SchemaBuilder.fixed("fixed").size(1),
              "Byte array with length 3 exceeds fixed type with size 1"
            )
          }

          it("should encode as fixed") {
            val schema = SchemaBuilder.fixed("fixed").size(2)

            implicit val codec: Codec[Array[Byte]] =
              Codec.instance(
                Right(schema),
                Codec.bytes.encode,
                Codec.bytes.decode
              )

            assertEncodeIs[Array[Byte]](
              Array(1),
              Right {
                GenericData
                  .get()
                  .createFixed(null, {
                    ByteBuffer.allocate(2).put(Array[Byte](1)).array()
                  }, schema)
              }
            )
          }
        }
      }

      describe("decode") {
        it("should error on non-fixed, non-bytes schema") {
          assertDecodeError[Array[Byte]](
            unsafeEncode[Array[Byte]](Array(1)),
            unsafeSchema[String],
            "Got unexpected schema type STRING while decoding Array[Byte], expected schema types BYTES or FIXED"
          )
        }

        it("should error if value is not ByteBuffer") {
          assertDecodeError[Array[Byte]](
            10,
            unsafeSchema[Array[Byte]],
            "Got unexpected type java.lang.Integer while decoding Array[Byte], expected type ByteBuffer"
          )
        }

        it("should decode as bytes") {
          assertDecodeIs[Array[Byte]](
            unsafeEncode[Array[Byte]](Array(1)),
            Right(Array[Byte](1))
          )
        }

        describe("fixed") {
          it("should error if bytes exceed schema size") {
            assertDecodeError[Array[Byte]](
              unsafeEncode[Array[Byte]](Array(1, 2, 3))(
                Codec.instance(
                  Right(SchemaBuilder.fixed("fixed").size(3)),
                  Codec[Array[Byte]].encode,
                  Codec[Array[Byte]].decode
                )
              ),
              SchemaBuilder.fixed("fixed").size(1),
              "Byte array with length 3 exceeds fixed type with size 1"
            )
          }

          it("should error if value is not GenericFixed") {
            assertDecodeError[Array[Byte]](
              10,
              SchemaBuilder.fixed("fixed").size(1),
              "Got unexpected type java.lang.Integer while decoding Array[Byte], expected type GenericFixed"
            )
          }

          it("should decode as fixed") {
            implicit val codec: Codec[Array[Byte]] =
              Codec.instance(
                Right(SchemaBuilder.fixed("fixed").size(2)),
                Codec.bytes.encode,
                Codec.bytes.decode
              )

            assertDecodeIs[Array[Byte]](
              unsafeEncode[Array[Byte]](Array(1)),
              Right(Array[Byte](1, 0))
            )
          }
        }
      }
    }

    describe("chain") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[Chain[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[Chain[Int]](
            Chain(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Chain, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[Chain[Int]](
            Chain(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[Chain[Int]](
            unsafeEncode(Chain(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Chain, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Chain[Int]](
            unsafeEncode(10),
            unsafeSchema[Chain[Int]],
            "Got unexpected type java.lang.Integer while decoding Chain, expected type Collection"
          )
        }

        it("should decode as Chain") {
          val value = Chain(1, 2, 3)
          assertDecodeIs[Chain[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

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

        it("should encode first in coproduct using first type") {
          type A = Int :+: String :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A](123)),
            Right(Coproduct[A](123))
          )
        }

        it("should encode second in coproduct using second type") {
          type A = Int :+: String :+: CNil
          assertDecodeIs[A](
            unsafeEncode(Coproduct[A]("abc")),
            Right(Coproduct[A]("abc"))
          )
        }
      }
    }

    describe("decimal") {
      describe("schema") {
        it("should be encoded as bytes with logical type decimal") {
          implicit val codec: Codec[BigDecimal] =
            Codec.decimal(precision = 10, scale = 5)

          assertSchemaIs[BigDecimal] {
            """{"type":"bytes","logicalType":"decimal","precision":10,"scale":5}"""
          }
        }

        it("should capture errors on invalid precision and scale") {
          implicit val avroSchema: Codec[BigDecimal] =
            Codec.decimal(precision = -1, scale = -1)

          assertSchemaError[BigDecimal] {
            """java.lang.IllegalArgumentException: Invalid decimal precision: -1 (must be positive)"""
          }
        }
      }

      describe("encode") {
        implicit val codec: Codec[BigDecimal] =
          Codec.decimal(precision = 10, scale = 5)

        it("should error if schema is not bytes") {
          assertEncodeError[BigDecimal](
            BigDecimal("123"),
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding BigDecimal, expected schema type BYTES"
          )
        }

        it("should error if logical type is missing") {
          assertEncodeError[BigDecimal](
            BigDecimal("123"),
            SchemaBuilder.builder().bytesType(),
            "Got unexpected missing logical type while encoding BigDecimal"
          )
        }

        it("should error if logical type is not decimal") {
          assertEncodeError[BigDecimal](
            BigDecimal("123"), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.uuid().addToSchema(bytes)
            },
            "Got unexpected logical type uuid while encoding BigDecimal"
          )
        }

        it("should error if scale is different in schema") {
          assertEncodeError[BigDecimal](
            BigDecimal("123"), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.decimal(10, 5).addToSchema(bytes)
            },
            "Unable to encode decimal with scale 0 as scale 5"
          )
        }

        it("should error if precision exceeds schema precision") {
          assertEncodeError[BigDecimal](
            BigDecimal("123456.45678"), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.decimal(10, 5).addToSchema(bytes)
            },
            "Unable to encode decimal with precision 11 exceeding schema precision 10"
          )
        }

        it("should encode as bytes") {
          implicit val codec: Codec[BigDecimal] =
            Codec.decimal(precision = 10, scale = 5)

          val value = BigDecimal("123.45678")
          assertEncodeIs[BigDecimal](
            value,
            Right {
              new Conversions.DecimalConversion().toBytes(
                value.underlying(),
                unsafeSchema[BigDecimal],
                unsafeSchema[BigDecimal].getLogicalType()
              )
            }
          )
        }
      }

      describe("decode") {
        implicit val codec: Codec[BigDecimal] =
          Codec.decimal(precision = 10, scale = 5)

        it("should error if schema is not bytes") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")),
            unsafeSchema[String],
            "Got unexpected schema type STRING while decoding BigDecimal, expected schema type BYTES"
          )
        }

        it("should error if value is not byte buffer") {
          assertDecodeError[BigDecimal](
            unsafeEncode(10),
            unsafeSchema[BigDecimal],
            "Got unexpected type java.lang.Integer while decoding BigDecimal, expected type ByteBuffer"
          )
        }

        it("should error if logical type is missing") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")),
            SchemaBuilder.builder().bytesType(),
            "Got unexpected missing logical type while decoding BigDecimal"
          )
        }

        it("should error if logical type is not decimal") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.uuid().addToSchema(bytes)
            },
            "Got unexpected logical type uuid while decoding BigDecimal"
          )
        }

        it("should error if precision exceeds schema precision") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.decimal(6, 5).addToSchema(bytes)
            },
            "Unable to decode decimal with precision 8 exceeding schema precision 6"
          )
        }

        it("should decode bytes") {
          implicit val codec: Codec[BigDecimal] =
            Codec.decimal(precision = 10, scale = 5)

          val value = BigDecimal("123.45678")
          assertDecodeIs[BigDecimal](
            unsafeEncode(value),
            Right(value)
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

          it("should error if value is not generic container") {
            assertDecodeError[SealedTraitCaseClass](
              unsafeEncode(123),
              unsafeSchema[SealedTraitCaseClass],
              "Got unexpected type java.lang.Integer while decoding vulcan.examples.SealedTraitCaseClass, expected type GenericContainer"
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
              "Missing subtype vulcan.examples.CaseObjectInSealedTrait in union for type vulcan.examples.SealedTraitCaseClass"
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

    describe("deriveEnum") {
      describe("schema") {
        it("should derive name, namespace, doc") {
          implicit val codec: Codec[CaseClassAvroDoc] =
            Codec.deriveEnum(
              symbols = List("first"),
              encode = _ => "first",
              decode = _ => Right(CaseClassAvroDoc(None))
            )

          assertSchemaIs[CaseClassAvroDoc] {
            """{"type":"enum","name":"CaseClassAvroDoc","namespace":"vulcan.examples","doc":"documentation","symbols":["first"]}"""
          }
        }
      }
    }

    describe("double") {
      describe("schema") {
        it("should be encoded as double") {
          assertSchemaIs[Double] {
            """"double""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not double") {
          assertEncodeError[Double](
            123d,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Double, expected schema type DOUBLE"
          )
        }

        it("should encode as double") {
          val value = 123d
          assertEncodeIs[Double](
            value,
            Right(java.lang.Double.valueOf(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not double") {
          assertDecodeError[Double](
            unsafeEncode(123d),
            unsafeSchema[Long],
            "Got unexpected schema type LONG while decoding Double, expected schema type DOUBLE"
          )
        }

        it("should error if value is not double") {
          assertDecodeError[Double](
            unsafeEncode(10L),
            unsafeSchema[Double],
            "Got unexpected type java.lang.Long while decoding Double, expected type Double"
          )
        }

        it("should decode as Double") {
          val value = 123d
          assertDecodeIs[Double](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("enum") {
      describe("schema") {
        it("should have expected schema") {
          assertSchemaIs[SealedTraitEnum] {
            """{"type":"enum","name":"SealedTraitEnum","namespace":"vulcan.examples","doc":"documentation","symbols":["first","second"],"default":"first","aliases":["first","second"]}"""
          }
        }

        it("should capture errors on invalid schema") {
          assertSchemaError[`-SealedTraitEnumInvalidName`] {
            """org.apache.avro.SchemaParseException: Illegal initial character: -SealedTraitEnumInvalidName"""
          }
        }
      }

      describe("encode") {
        it("should error if schema type is not enum") {
          assertEncodeError[SealedTraitEnum](
            FirstInSealedTraitEnum,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding vulcan.examples.SealedTraitEnum, expected schema type ENUM"
          )
        }

        it("should error if schema full name doesn't match type name") {
          assertEncodeError[SealedTraitEnum](
            FirstInSealedTraitEnum,
            SchemaBuilder.builder().enumeration("the.enum").symbols(),
            "Unable to encode vulcan.examples.SealedTraitEnum using schema with name the.enum since names do not match"
          )
        }

        it("should error if returned string is not a schema symbol") {
          implicit val codec: Codec[SealedTraitEnum] =
            Codec.enum(
              name = "SealedTraitEnum",
              namespace = Some("vulcan.examples"),
              symbols = List("symbol"),
              encode = _ => "not-symbol",
              decode = _ => Left(AvroError("error"))
            )

          assertEncodeError[SealedTraitEnum](
            FirstInSealedTraitEnum,
            unsafeSchema[SealedTraitEnum],
            "Symbol not-symbol is not part of schema symbols [symbol] for type vulcan.examples.SealedTraitEnum"
          )
        }

        it("should encode a valid symbol") {
          assertEncodeIs[SealedTraitEnum](
            FirstInSealedTraitEnum,
            Right(new GenericData.EnumSymbol(unsafeSchema[SealedTraitEnum], "first"))
          )
        }
      }

      describe("decode") {
        it("should error if schema type is not enum") {
          assertDecodeError[SealedTraitEnum](
            unsafeEncode[SealedTraitEnum](FirstInSealedTraitEnum),
            unsafeSchema[String],
            "Got unexpected schema type STRING while decoding vulcan.examples.SealedTraitEnum, expected schema type ENUM"
          )
        }

        it("should error if value is not GenericEnumSymbol") {
          assertDecodeError[SealedTraitEnum](
            unsafeEncode(10),
            unsafeSchema[SealedTraitEnum],
            "Got unexpected type java.lang.Integer while decoding vulcan.examples.SealedTraitEnum, expected type GenericEnumSymbol"
          )
        }

        it("should error if schema full name doesn't match type name") {
          assertDecodeError[SealedTraitEnum](
            unsafeEncode[SealedTraitEnum](FirstInSealedTraitEnum),
            SchemaBuilder.builder().enumeration("the.enum").symbols(),
            "Unable to decode vulcan.examples.SealedTraitEnum using schema with name the.enum since names do not match"
          )
        }

        it("should error if encoded value is not a schema symbol") {
          assertDecodeError[SealedTraitEnum](
            new GenericData.EnumSymbol(
              SchemaBuilder
                .builder()
                .enumeration("vulcan.examples.SealedTraitEnum")
                .symbols("symbol"),
              "symbol"
            ),
            unsafeSchema[SealedTraitEnum],
            "symbol is not part of schema symbols [first, second] for type vulcan.examples.SealedTraitEnum"
          )
        }

        it("should decode a valid symbol") {
          assertDecodeIs[SealedTraitEnum](
            unsafeEncode[SealedTraitEnum](FirstInSealedTraitEnum),
            Right(FirstInSealedTraitEnum)
          )
        }
      }
    }

    describe("float") {
      describe("schema") {
        it("should be encoded as float") {
          assertSchemaIs[Float] {
            """"float""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not float") {
          assertEncodeError[Float](
            123f,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Float, expected schema type FLOAT"
          )
        }

        it("should encode as float") {
          val value = 123f
          assertEncodeIs[Float](
            value,
            Right(java.lang.Float.valueOf(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not float") {
          assertDecodeError[Float](
            unsafeEncode(123f),
            unsafeSchema[Long],
            "Got unexpected schema type LONG while decoding Float, expected schema type FLOAT"
          )
        }

        it("should error if value is not float") {
          assertDecodeError[Float](
            unsafeEncode(10L),
            unsafeSchema[Float],
            "Got unexpected type java.lang.Long while decoding Float, expected type Float"
          )
        }

        it("should decode as Float") {
          val value = 123f
          assertDecodeIs[Float](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("instant") {
      describe("schema") {
        it("should be encoded as long with logical type timestamp-millis") {
          assertSchemaIs[Instant] {
            """{"type":"long","logicalType":"timestamp-millis"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not long") {
          assertEncodeError[Instant](
            Instant.now(),
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Instant, expected schema type LONG"
          )
        }

        it("should error if logical type is missing") {
          assertEncodeError[Instant](
            Instant.now(),
            unsafeSchema[Long],
            "Got unexpected missing logical type while encoding Instant"
          )
        }

        it("should error if logical type is not timestamp-millis") {
          assertEncodeError[Instant](
            Instant.now(), {
              LogicalTypes.timestampMicros().addToSchema {
                SchemaBuilder.builder().longType()
              }
            },
            "Got unexpected logical type timestamp-micros while encoding Instant"
          )
        }

        it("should encode as long") {
          val value = {
            val instant = Instant.now()
            instant.minusNanos(instant.getNano().toLong)
          }

          assertEncodeIs[Instant](
            value,
            Right(java.lang.Long.valueOf(value.toEpochMilli()))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not long") {
          assertDecodeError[Instant](
            unsafeEncode(Instant.now()),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Instant, expected schema type LONG"
          )
        }

        it("should error if logical type is missing") {
          assertDecodeError[Instant](
            unsafeEncode(Instant.now()),
            unsafeSchema[Long],
            "Got unexpected missing logical type while decoding Instant"
          )
        }

        it("should error if logical type is not timestamp-millis") {
          assertDecodeError[Instant](
            unsafeEncode(Instant.now()), {
              LogicalTypes.timestampMicros().addToSchema {
                SchemaBuilder.builder().longType()
              }
            },
            "Got unexpected logical type timestamp-micros while decoding Instant"
          )
        }

        it("should error if value is not long") {
          assertDecodeError[Instant](
            unsafeEncode(123),
            unsafeSchema[Instant],
            "Got unexpected type java.lang.Integer while decoding Instant, expected type Long"
          )
        }

        it("should decode as Instant") {
          val value = {
            val instant = Instant.now()
            instant.minusNanos(instant.getNano().toLong)
          }

          assertDecodeIs[Instant](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("int") {
      describe("schema") {
        it("should be encoded as int") {
          assertSchemaIs[Int] {
            """"int""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not int") {
          assertEncodeError[Int](
            123,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Int, expected schema type INT"
          )
        }

        it("should encode as int") {
          val value = 123
          assertEncodeIs[Int](
            value,
            Right(java.lang.Integer.valueOf(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int") {
          assertDecodeError[Int](
            unsafeEncode(123),
            unsafeSchema[Long],
            "Got unexpected schema type LONG while decoding Int, expected schema type INT"
          )
        }

        it("should error if value is not int") {
          assertDecodeError[Int](
            unsafeEncode(10L),
            unsafeSchema[Int],
            "Got unexpected type java.lang.Long while decoding Int, expected type Int"
          )
        }

        it("should decode as Int") {
          val value = 123
          assertDecodeIs[Int](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("list") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[List[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[List[Int]](
            List(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding List, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[List[Int]](
            List(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[List[Int]](
            unsafeEncode(List(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding List, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[List[Int]](
            unsafeEncode(10),
            unsafeSchema[List[Int]],
            "Got unexpected type java.lang.Integer while decoding List, expected type Collection"
          )
        }

        it("should decode as List") {
          val value = List(1, 2, 3)
          assertDecodeIs[List[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("localDate") {
      describe("schema") {
        it("should be encoded as int with logical type date") {
          assertSchemaIs[LocalDate] {
            """{"type":"int","logicalType":"date"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not int") {
          assertEncodeError[LocalDate](
            LocalDate.now(),
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding LocalDate, expected schema type INT"
          )
        }

        it("should error if logical type is missing") {
          assertEncodeError[LocalDate](
            LocalDate.now(),
            unsafeSchema[Int],
            "Got unexpected missing logical type while encoding LocalDate"
          )
        }

        it("should encode as int") {
          val value = LocalDate.now()
          assertEncodeIs[LocalDate](
            value,
            Right(java.lang.Integer.valueOf(value.toEpochDay().toInt))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int") {
          assertDecodeError[LocalDate](
            unsafeEncode(LocalDate.now()),
            unsafeSchema[Long],
            "Got unexpected schema type LONG while decoding LocalDate, expected schema type INT"
          )
        }

        it("should error if logical type is not date") {
          assertDecodeError[LocalDate](
            unsafeEncode(LocalDate.now()),
            unsafeSchema[Int],
            "Got unexpected missing logical type while decoding LocalDate"
          )
        }

        it("should error if value is not int") {
          assertDecodeError[LocalDate](
            unsafeEncode(123L),
            unsafeSchema[LocalDate],
            "Got unexpected type java.lang.Long while decoding LocalDate, expected type Integer"
          )
        }

        it("should decode int as local date") {
          val value = LocalDate.now()
          assertDecodeIs[LocalDate](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("long") {
      describe("schema") {
        it("should be encoded as long") {
          assertSchemaIs[Long] {
            """"long""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not long") {
          assertEncodeError[Long](
            123L,
            unsafeSchema[String],
            "Got unexpected schema type STRING while encoding Long, expected schema type LONG"
          )
        }

        it("should encode as long") {
          val value = 123L
          assertEncodeIs[Long](
            value,
            Right(java.lang.Long.valueOf(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not long") {
          assertDecodeError[Long](
            unsafeEncode(123L),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Long, expected schema type LONG"
          )
        }

        it("should error if value is not long") {
          assertDecodeError[Long](
            unsafeEncode(10),
            unsafeSchema[Long],
            "Got unexpected type java.lang.Integer while decoding Long, expected type Long"
          )
        }

        it("should decode utf8 as string") {
          val value = 123L
          assertDecodeIs[Long](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("nonEmptyChain") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[NonEmptyChain[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[NonEmptyChain[Int]](
            NonEmptyChain(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding NonEmptyChain, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[NonEmptyChain[Int]](
            NonEmptyChain(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[NonEmptyChain[Int]](
            unsafeEncode(NonEmptyChain(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding NonEmptyChain, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyChain[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyChain[Int]],
            "Got unexpected type java.lang.Integer while decoding NonEmptyChain, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyChain[Int]](
            unsafeEncode(Chain.empty[Int]),
            unsafeSchema[NonEmptyChain[Int]],
            "Got unexpected empty collection while decoding NonEmptyChain"
          )
        }

        it("should decode as NonEmptyChain") {
          val value = NonEmptyChain(1, 2, 3)
          assertDecodeIs[NonEmptyChain[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("nonEmptyList") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[NonEmptyList[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[NonEmptyList[Int]](
            NonEmptyList.of(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding NonEmptyList, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[NonEmptyList[Int]](
            NonEmptyList.of(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[NonEmptyList[Int]](
            unsafeEncode(NonEmptyList.of(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding NonEmptyList, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyList[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyList[Int]],
            "Got unexpected type java.lang.Integer while decoding NonEmptyList, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyList[Int]](
            unsafeEncode(List.empty[Int]),
            unsafeSchema[NonEmptyList[Int]],
            "Got unexpected empty collection while decoding NonEmptyList"
          )
        }

        it("should decode as NonEmptyList") {
          val value = NonEmptyList.of(1, 2, 3)
          assertDecodeIs[NonEmptyList[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("nonEmptySet") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[NonEmptySet[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[NonEmptySet[Int]](
            NonEmptySet.of(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding NonEmptySet, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[NonEmptySet[Int]](
            NonEmptySet.of(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[NonEmptySet[Int]](
            unsafeEncode(NonEmptySet.of(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding NonEmptySet, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptySet[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptySet[Int]],
            "Got unexpected type java.lang.Integer while decoding NonEmptySet, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptySet[Int]](
            unsafeEncode(Set.empty[Int]),
            unsafeSchema[NonEmptySet[Int]],
            "Got unexpected empty collection while decoding NonEmptySet"
          )
        }

        it("should decode as NonEmptySet") {
          val value = NonEmptySet.of(1, 2, 3)
          assertDecodeIs[NonEmptySet[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("nonEmptyVector") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[NonEmptyVector[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[NonEmptyVector[Int]](
            NonEmptyVector.of(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding NonEmptyVector, expected schema type ARRAY"
          )
        }

        it("should encode as java vector using encoder for underlying type") {
          assertEncodeIs[NonEmptyVector[Int]](
            NonEmptyVector.of(1, 2, 3),
            Right(Vector(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[NonEmptyVector[Int]](
            unsafeEncode(NonEmptyVector.of(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding NonEmptyVector, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyVector[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyVector[Int]],
            "Got unexpected type java.lang.Integer while decoding NonEmptyVector, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyVector[Int]](
            unsafeEncode(Vector.empty[Int]),
            unsafeSchema[NonEmptyVector[Int]],
            "Got unexpected empty collection while decoding NonEmptyVector"
          )
        }

        it("should decode as NonEmptyVector") {
          val value = NonEmptyVector.of(1, 2, 3)
          assertDecodeIs[NonEmptyVector[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("option") {
      describe("schema") {
        it("should be encoded as union") {
          assertSchemaIs[Option[Double]] {
            """["null","double"]"""
          }
        }

        it("should catpure errors on nested unions") {
          assertSchemaError[Option[Option[Int]]] {
            """org.apache.avro.AvroRuntimeException: Nested union: ["null",["null","int"]]"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not union") {
          assertEncodeError[Option[Int]](
            Some(1),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Option, expected schema type UNION"
          )
        }

        it("should error if there is one schema in union") {
          assertEncodeError[Option[Int]](
            Some(1), {
              SchemaBuilder.unionOf().intType().endUnion()
            },
            """Unexpected union schema ["int"] for Option"""
          )
        }

        it("should error if there are more than two schemas in union") {
          assertEncodeError[Option[Int]](
            Some(1), {
              SchemaBuilder
                .unionOf()
                .intType()
                .and()
                .stringType()
                .and()
                .nullType()
                .endUnion()
            },
            """Unexpected union schema ["int","string","null"] for Option"""
          )
        }

        it("should support null as first schema type in union") {
          implicit val codec: Codec[Option[Int]] =
            Codec.instance(
              Right {
                SchemaBuilder
                  .unionOf()
                  .nullType()
                  .and()
                  .intType()
                  .endUnion()
              },
              Codec.option[Int].encode,
              Codec.option[Int].decode
            )

          assertEncodeIs[Option[Int]](
            Some(1),
            Right(unsafeEncode(1))
          )
        }

        it("should support null as second schema type in union") {
          implicit val codec: Codec[Option[Int]] =
            Codec.instance(
              Right {
                SchemaBuilder
                  .unionOf()
                  .intType()
                  .and()
                  .nullType()
                  .endUnion()
              },
              Codec.option[Int].encode,
              Codec.option[Int].decode
            )

          assertEncodeIs[Option[Int]](
            Some(1),
            Right(unsafeEncode(1))
          )
        }

        it("should encode None as null") {
          assertEncodeIs[Option[Int]](
            None,
            Right(null)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not union") {
          assertDecodeError[Option[Int]](
            unsafeEncode(Option(1)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Option, expected schema type UNION"
          )
        }

        it("should error if there is one schema in union") {
          assertDecodeError[Option[Int]](
            unsafeEncode(Option(1)), {
              SchemaBuilder.unionOf().intType().endUnion()
            },
            """Unexpected union schema ["int"] for Option"""
          )
        }

        it("should error if there are more than two schemas in union") {
          assertDecodeError[Option[Int]](
            unsafeEncode(Option(1)), {
              SchemaBuilder.unionOf().intType().and().stringType().and().nullType().endUnion()
            },
            """Unexpected union schema ["int","string","null"] for Option"""
          )
        }

        it("should support null as first schema type in union") {
          implicit val codec: Codec[Option[Int]] =
            Codec.instance(
              Right {
                SchemaBuilder
                  .unionOf()
                  .nullType()
                  .and()
                  .intType()
                  .endUnion()
              },
              Codec.option[Int].encode,
              Codec.option[Int].decode
            )

          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1))
          )
        }

        it("should support null as second schema type in union") {
          implicit val codec: Codec[Option[Int]] =
            Codec.instance(
              Right {
                SchemaBuilder
                  .unionOf()
                  .intType()
                  .and()
                  .nullType()
                  .endUnion()
              },
              Codec.option[Int].encode,
              Codec.option[Int].decode
            )

          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1))
          )
        }

        it("should decode null as None") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option.empty[Int]),
            Right(None)
          )
        }
      }
    }

    describe("record") {
      implicit val recordCodec: Codec[CaseClassTwoFields] =
        Codec.record(
          name = "CaseClassTwoFields",
          namespace = Some("vulcan.examples"),
          doc = Some("some documentation for example"),
          aliases = Seq("FirstAlias", "SecondAlias"),
          props = Seq("custom" -> "custom record value")
        ) { field =>
          assert(field.toString() == "FieldBuilder")

          (
            field(
              name = "name",
              access = _.name,
              doc = Some("some doc"),
              default = Some("default name"),
              order = Some(Schema.Field.Order.DESCENDING),
              aliases = Seq("TheAlias"),
              props = Seq("custom" -> "custom field value")
            ),
            field("age", _.age)
          ).mapN(CaseClassTwoFields(_, _))
        }

      describe("schema") {
        it("should have the expected schema") {
          assert {
            recordCodec.schema.right.value.toString() ==
              """{"type":"record","name":"CaseClassTwoFields","namespace":"vulcan.examples","doc":"some documentation for example","fields":[{"name":"name","type":"string","doc":"some doc","default":"default name","order":"descending","aliases":["TheAlias"],"custom":"custom field value"},{"name":"age","type":"int"}],"custom":"custom record value","aliases":["FirstAlias","SecondAlias"]}"""
          }
        }

        it("should error if default value can't be encoded") {
          implicit val intCodec: Codec[Int] =
            Codec.instance(
              Codec.int.schema,
              (_, _) => Left(AvroError("error")),
              (_, _) => Left(AvroError("error"))
            )

          implicit val caseClassFieldCodec: Codec[CaseClassField] =
            Codec.record[CaseClassField]("CaseClassField") { field =>
              field("value", _.value, default = Some(10)).map(CaseClassField(_))
            }

          assert(caseClassFieldCodec.schema.left.value.message == "error")
        }

        it("should error if default value is not valid for field") {
          implicit val intCodec: Codec[Int] =
            Codec.instance(
              Codec.int.schema,
              (_, _) => Right("invalid"),
              (_, _) => Left(AvroError("error"))
            )

          implicit val caseClassFieldCodec: Codec[CaseClassField] =
            Codec.record[CaseClassField]("CaseClassField") { field =>
              field("value", _.value, default = Some(10)).map(CaseClassField(_))
            }

          assert {
            caseClassFieldCodec.schema.left.value.message ==
              """org.apache.avro.AvroTypeException: Invalid default for field value: "invalid" not a "int""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not record") {
          assertEncodeError[CaseClassTwoFields](
            CaseClassTwoFields("name", 0),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding vulcan.examples.CaseClassTwoFields, expected schema type RECORD"
          )
        }

        it("should error if record schema name does not match") {
          assertEncodeError[CaseClassTwoFields](
            CaseClassTwoFields("name", 0),
            unsafeSchema[CaseClassField],
            "Unable to encode vulcan.examples.CaseClassTwoFields using schema with name vulcan.examples.CaseClassField since names do not match"
          )
        }

        it("should error if record schema is missing any field") {
          assertEncodeError[CaseClassTwoFields](
            CaseClassTwoFields("name", 0), {
              SchemaBuilder
                .record("vulcan.examples.CaseClassTwoFields")
                .fields()
                .endRecord()
            },
            "Record field 'name' in schema is missing for type vulcan.examples.CaseClassTwoFields"
          )
        }

        it("should encode as record") {
          assertEncodeIs[CaseClassTwoFields](
            CaseClassTwoFields("name", 0),
            Right {
              val record = new GenericData.Record(unsafeSchema[CaseClassTwoFields])
              record.put(0, unsafeEncode("name"))
              record.put(1, unsafeEncode(0))
              record
            }
          )
        }
      }

      describe("decode") {
        it("should error if schema is not record") {
          assertDecodeError[CaseClassTwoFields](
            unsafeEncode(CaseClassTwoFields("name", 123)),
            unsafeSchema[String],
            "Got unexpected schema type STRING while decoding vulcan.examples.CaseClassTwoFields, expected schema type RECORD"
          )
        }

        it("should error if value is not indexed record") {
          assertDecodeError[CaseClassTwoFields](
            unsafeEncode(123),
            unsafeSchema[CaseClassTwoFields],
            "Got unexpected type java.lang.Integer while decoding vulcan.examples.CaseClassTwoFields, expected type IndexedRecord"
          )
        }

        it("should error if writer schema full name does not match") {
          assertDecodeError[CaseClassTwoFields](
            {
              val schema =
                Schema.createRecord("Record", null, "com.xyz", false)

              schema.setFields(
                List(
                  new Schema.Field(
                    "name",
                    unsafeSchema[String],
                    null
                  ),
                  new Schema.Field(
                    "age",
                    unsafeSchema[Int],
                    null
                  )
                ).asJava
              )

              val record = new GenericData.Record(schema)
              record.put(0, "name")
              record.put(1, 123)
              record
            },
            unsafeSchema[CaseClassTwoFields],
            "Got record writer schema with name com.xyz.Record, expected name vulcan.examples.CaseClassTwoFields"
          )
        }

        it("should error if any field without default value is missing") {
          assertDecodeError[CaseClassTwoFields](
            {
              val schema =
                Schema.createRecord("CaseClassTwoFields", null, "vulcan.examples", false)

              schema.setFields(
                List(
                  new Schema.Field(
                    "name",
                    unsafeSchema[String],
                    null
                  )
                ).asJava
              )

              val record = new GenericData.Record(schema)
              record.put(0, unsafeEncode("name"))
              record
            },
            unsafeSchema[CaseClassTwoFields],
            "Record writer schema is missing field 'age' while decoding vulcan.examples.CaseClassTwoFields"
          )
        }

        it("should decode if field with default value is missing") {
          assertDecodeIs[CaseClassTwoFields](
            {
              val schema =
                Schema.createRecord("CaseClassTwoFields", null, "vulcan.examples", false)

              schema.setFields(
                List(
                  new Schema.Field(
                    "age",
                    unsafeSchema[Int],
                    null
                  )
                ).asJava
              )

              val record = new GenericData.Record(schema)
              record.put(0, 123)
              record
            },
            Right(CaseClassTwoFields("default name", 123))
          )
        }

        it("should decode as case class") {
          assertDecodeIs[CaseClassTwoFields](
            unsafeEncode(CaseClassTwoFields("name", 123)),
            Right(CaseClassTwoFields("name", 123))
          )
        }
      }
    }

    describe("seq") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[Seq[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[Seq[Int]](
            Seq(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Seq, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[Seq[Int]](
            Seq(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[Seq[Int]](
            unsafeEncode(Seq(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Seq, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Seq[Int]](
            unsafeEncode(10),
            unsafeSchema[Seq[Int]],
            "Got unexpected type java.lang.Integer while decoding Seq, expected type Collection"
          )
        }

        it("should decode as Seq") {
          val value = Seq(1, 2, 3)
          assertDecodeIs[Seq[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("show") {
      it("should include the schema if available") {
        assert {
          Codec[Int].show == """Codec("int")"""
        }
      }

      it("should show the error if schema is unavailable") {
        assert {
          Codec
            .instance(
              Left(AvroError("error")),
              Codec.int.encode,
              Codec.int.decode
            )
            .show == "AvroError(error)"
        }
      }

      it("should have a Show instance consistent with toString") {
        val codec = Codec[Boolean]
        assert {
          codec.show == codec.toString()
        }
      }
    }

    describe("set") {
      describe("schema") {
        it("should be encoded as array") {
          assertSchemaIs[Set[String]] {
            """{"type":"array","items":"string"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[Set[Int]](
            Set(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Set, expected schema type ARRAY"
          )
        }

        it("should encode as java list using encoder for underlying type") {
          assertEncodeIs[Set[Int]](
            Set(1, 2, 3),
            Right(List(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[Set[Int]](
            unsafeEncode(Set(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Set, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Set[Int]](
            unsafeEncode(10),
            unsafeSchema[Set[Int]],
            "Got unexpected type java.lang.Integer while decoding Set, expected type Collection"
          )
        }

        it("should decode as Set") {
          val value = Set(1, 2, 3)
          assertDecodeIs[Set[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("string") {
      describe("schema") {
        it("should be encoded as string") {
          assertSchemaIs[String] {
            """"string""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not string") {
          assertEncodeError[String](
            "abc",
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding String, expected schema type STRING"
          )
        }

        it("should encode as utf8") {
          val value = "abc"
          assertEncodeIs[String](
            value,
            Right(new Utf8(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not string") {
          assertDecodeError[String](
            unsafeEncode("abc"),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding String, expected schema type STRING"
          )
        }

        it("should error if value is not utf8") {
          assertDecodeError[String](
            unsafeEncode(10),
            unsafeSchema[String],
            "Got unexpected type java.lang.Integer while decoding String, expected type Utf8"
          )
        }

        it("should decode utf8 as string") {
          val value = "abc"
          assertDecodeIs[String](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("unit") {
      describe("schema") {
        it("should be encoded as null") {
          assertSchemaIs[Unit] {
            """"null""""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not null") {
          assertEncodeError[Unit](
            (),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Unit, expected schema type NULL"
          )
        }

        it("should encode as null") {
          val value = ()
          assertEncodeIs[Unit](
            value,
            Right(null)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not null") {
          assertDecodeError[Unit](
            unsafeEncode(()),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Unit, expected schema type NULL"
          )
        }

        it("should error if value is not null") {
          assertDecodeError[Unit](
            unsafeEncode(10),
            unsafeSchema[Unit],
            "Got unexpected type java.lang.Integer while decoding Unit, expected type Null"
          )
        }

        it("should decode null as unit") {
          assertDecodeIs[Unit](
            unsafeEncode(()),
            Right(())
          )
        }
      }
    }

    describe("uuid") {
      describe("schema") {
        it("should be encoded as string with logical type uuid") {
          assertSchemaIs[UUID] {
            """{"type":"string","logicalType":"uuid"}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not string") {
          assertEncodeError[UUID](
            UUID.randomUUID(),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding UUID, expected schema type STRING"
          )
        }

        it("should error if no logical type") {
          assertEncodeError[UUID](
            UUID.randomUUID(),
            unsafeSchema[String],
            "Got unexpected missing logical type while encoding UUID"
          )
        }

        it("should encode as utf8") {
          val value = UUID.randomUUID()
          assertEncodeIs[UUID](
            value,
            Right(new Utf8(value.toString()))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not string") {
          assertDecodeError[UUID](
            unsafeEncode(UUID.randomUUID()),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding UUID, expected schema type STRING"
          )
        }

        it("should error if logical type is not uuid") {
          assertDecodeError[UUID](
            unsafeEncode(UUID.randomUUID()),
            unsafeSchema[String],
            "Got unexpected missing logical type while decoding UUID"
          )
        }

        it("should error if value is not utf8") {
          assertDecodeError[UUID](
            10,
            unsafeSchema[UUID],
            "Got unexpected type java.lang.Integer while decoding UUID, expected type Utf8"
          )
        }

        it("should error if value is not uuid") {
          assertDecodeError[UUID](
            new Utf8("not-uuid"),
            unsafeSchema[UUID],
            "java.lang.IllegalArgumentException: Invalid UUID string: not-uuid"
          )
        }

        it("should decode uuid") {
          val value = UUID.randomUUID()
          assertDecodeIs[UUID](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("vector") {
      describe("schema") {
        it("should be encoded as an array") {
          assertSchemaIs[Vector[Option[Long]]] {
            """{"type":"array","items":["null","long"]}"""
          }
        }
      }

      describe("encode") {
        it("should error if schema is not array") {
          assertEncodeError[Vector[Int]](
            Vector(1, 2, 3),
            unsafeSchema[Int],
            "Got unexpected schema type INT while encoding Vector, expected schema type ARRAY"
          )
        }

        it("should encode as java vector using encoder for underlying type") {
          assertEncodeIs[Vector[Int]](
            Vector(1, 2, 3),
            Right(Vector(unsafeEncode(1), unsafeEncode(2), unsafeEncode(3)).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not array") {
          assertDecodeError[Vector[Int]](
            unsafeEncode(Vector(1, 2, 3)),
            unsafeSchema[Int],
            "Got unexpected schema type INT while decoding Vector, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Vector[Int]](
            unsafeEncode(10),
            unsafeSchema[Vector[Int]],
            "Got unexpected type java.lang.Integer while decoding Vector, expected type Collection"
          )
        }

        it("should decode as Vector") {
          val value = Vector(1, 2, 3)
          assertDecodeIs[Vector[Int]](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }
  }

  def unsafeSchema[A](implicit codec: Codec[A]): Schema =
    codec.schema.right.value

  def unsafeEncode[A](a: A)(implicit codec: Codec[A]): Any =
    codec.schema.flatMap(codec.encode(a, _)).right.value

  def unsafeDecode[A](value: Any)(implicit codec: Codec[A]): A =
    codec.schema.flatMap(codec.decode(value, _)).right.value

  def assertSchemaIs[A](expectedSchema: String)(implicit codec: Codec[A]): Assertion =
    assert(codec.schema.right.value.toString == expectedSchema)

  def assertEncodeIs[A](
    a: A,
    encoded: Either[AvroError, Any]
  )(implicit codec: Codec[A]): Assertion =
    assert(unsafeEncode(a) === encoded.right.value)

  def assertDecodeIs[A](
    value: Any,
    decoded: Either[AvroError, A]
  )(implicit codec: Codec[A]): Assertion =
    assert(unsafeDecode(value) === decoded.right.value)

  def assertSchemaError[A](
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.schema.left.value.message == expectedErrorMessage)

  def assertDecodeError[A](
    value: Any,
    schema: Schema,
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.decode(value, schema).left.value.message == expectedErrorMessage)

  def assertEncodeError[A](
    a: A,
    schema: Schema,
    expectedErrorMessage: String
  )(implicit codec: Codec[A]): Assertion =
    assert(codec.encode(a, schema).left.value.message == expectedErrorMessage)
}
