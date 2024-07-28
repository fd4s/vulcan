/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.data._
import cats.implicits._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalTime}
import java.util.concurrent.TimeUnit
import java.time.temporal.ChronoUnit
import java.util.UUID
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData
import org.scalacheck.Gen
import org.scalatest.Assertion
import vulcan.examples.{SecondInSealedTraitCaseClass, _}
import vulcan.internal.converters.collection._

import scala.util.{Failure, Success, Try}

final class CodecSpec extends BaseSpec with CodecSpecHelpers {
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
        it("should encode as boolean") {
          val value = true
          assertEncodeIs[Boolean](
            value,
            Right(value)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not boolean") {
          assertDecodeError[Boolean](
            unsafeEncode(false),
            unsafeSchema[Long],
            "Error decoding Boolean: Got unexpected schema type LONG, expected schema type BOOLEAN"
          )
        }

        it("should error if value is not boolean") {
          assertDecodeError[Boolean](
            unsafeEncode(10L),
            unsafeSchema[Boolean],
            "Error decoding Boolean: Got unexpected type java.lang.Long, expected type Boolean"
          )
        }

        it("should error if value is null") {
          assertDecodeError[Boolean](
            null,
            unsafeSchema[Boolean],
            "Error decoding Boolean: Got unexpected type null, expected type Boolean"
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

    describe("byte") {
      describe("schema") {
        it("should be encoded as int") {
          assertSchemaIs[Byte] {
            """"int""""
          }
        }
      }

      describe("encode") {
        it("should encode as int") {
          val value = 1.toByte
          assertEncodeIs[Byte](
            value,
            Right(1)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int") {
          assertDecodeError[Byte](
            unsafeEncode(1.toByte),
            unsafeSchema[String],
            "Error decoding Byte: Error decoding Int: Got unexpected schema type STRING, expected schema type INT"
          )
        }

        it("should error if value is not int") {
          assertDecodeError[Byte](
            unsafeEncode("value"),
            unsafeSchema[Byte],
            "Error decoding Byte: Error decoding Int: Got unexpected type org.apache.avro.util.Utf8, expected type Int"
          )
        }

        it("should error if int value is not byte") {
          val gen =
            Gen.oneOf(
              Gen.chooseNum(Int.MinValue, Byte.MinValue.toInt - 1),
              Gen.chooseNum(Byte.MaxValue.toInt + 1, Int.MaxValue)
            )

          forAll(gen) { nonByte =>
            assertDecodeError[Byte](
              unsafeEncode(nonByte),
              unsafeSchema[Byte],
              s"Error decoding Byte: Got unexpected Int value $nonByte, expected value in range -128 to 127"
            )
          }
        }

        it("should decode as byte") {
          forAll { (byte: Byte) =>
            assertDecodeIs[Byte](
              unsafeEncode(byte),
              Right(byte)
            )
          }
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
        it("should encode as bytes") {
          assertEncodeIs[Array[Byte]](
            Array(1),
            Right(ByteBuffer.wrap(Array(1)))
          )
        }
      }

      describe("decode") {
        it("should error on schema other than bytes or string") {
          assertDecodeError[Array[Byte]](
            unsafeEncode[Array[Byte]](Array(1)),
            unsafeSchema[Int],
            "Error decoding Array[Byte]: Got unexpected schema type INT, expected schema type BYTES"
          )
        }

        it("should error if value is not ByteBuffer") {
          assertDecodeError[Array[Byte]](
            10,
            unsafeSchema[Array[Byte]],
            "Error decoding Array[Byte]: Got unexpected type java.lang.Integer, expected type ByteBuffer"
          )
        }

        it("should decode string as bytes") {
          assertDecodeIs[Array[Byte]](
            unsafeEncode("foo"),
            Right("foo".getBytes(StandardCharsets.UTF_8))
          )
        }

        it("should decode bytes as bytes") {
          val str = "c6e6b318-303c-4ffa-8c0b-c689ddc49e94"
          val bytes = str.getBytes(StandardCharsets.UTF_8)
          val bb = StandardCharsets.UTF_8.encode(str)

          assertDecodeIs[Array[Byte]](
            bb,
            Right(bytes)
          )
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
            "Error decoding Chain: Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Chain[Int]](
            unsafeEncode(10),
            unsafeSchema[Chain[Int]],
            "Error decoding Chain: Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
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

    describe("char") {
      describe("schema") {
        it("should be encoded as string") {
          assertSchemaIs[Char] {
            """"string""""
          }
        }
      }

      describe("encode") {
        it("should encode as utf8") {
          val value = 'a'
          assertEncodeIs[Char](
            value,
            Right(Avro.String("a"))
          )
        }
      }

      describe("decode") {

        it("should error if utf8 value is empty") {
          assertDecodeError[Char](
            unsafeEncode(""),
            unsafeSchema[String],
            "Error decoding Char: Got unexpected String with length 0, expected length 1"
          )
        }

        it("should error if utf8 value has more than 1 char") {
          assertDecodeError[Char](
            unsafeEncode("ab"),
            unsafeSchema[String],
            "Error decoding Char: Got unexpected String with length 2, expected length 1"
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

        it("should error if scale is different in schema") {
          assertEncodeError[BigDecimal](
            BigDecimal("123"),
            "Error encoding BigDecimal: Unable to encode decimal with scale 0 as scale 5"
          )
        }

        it("should error if precision exceeds schema precision") {
          assertEncodeError[BigDecimal](
            BigDecimal("123456.45678"),
            "Error encoding BigDecimal: Unable to encode decimal with precision 11 exceeding schema precision 10"
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
            "Error decoding BigDecimal: Got unexpected schema type STRING, expected schema type BYTES"
          )
        }

        it("should error if value is not byte buffer") {
          assertDecodeError[BigDecimal](
            unsafeEncode(10),
            unsafeSchema[BigDecimal],
            "Error decoding BigDecimal: Got unexpected type java.lang.Integer, expected type ByteBuffer"
          )
        }

        it("should error if logical type is missing") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")),
            SchemaBuilder.builder().bytesType(),
            "Error decoding BigDecimal: Got unexpected missing logical type"
          )
        }

        it("should error if logical type is not decimal") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.uuid().addToSchema(bytes)
            },
            "Error decoding BigDecimal: Got unexpected logical type uuid"
          )
        }

        it("should error if precision exceeds schema precision") {
          assertDecodeError[BigDecimal](
            unsafeEncode(BigDecimal("123.45678")), {
              val bytes = SchemaBuilder.builder().bytesType()
              LogicalTypes.decimal(6, 5).addToSchema(bytes)
            },
            "Error decoding BigDecimal: Unable to decode decimal with precision 8 exceeding schema precision 6"
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

    describe("decode") {
      it("should decode using codec for type") {
        forAll { (n: Int) =>
          assert(Codec.decode[Int](n).value == n)
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
        it("should encode as double") {
          val value = 123d
          assertEncodeIs[Double](
            value,
            Right(value)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not double or promotable to double") {
          assertDecodeError[Double](
            unsafeEncode(123d),
            unsafeSchema[String],
            "Error decoding Double: Got unexpected schema type STRING, expected schema type DOUBLE"
          )
        }

        it("should error if value is not double or promotable to double") {
          assertDecodeError[Double](
            unsafeEncode("foo"),
            unsafeSchema[Double],
            "Error decoding Double: Got unexpected type org.apache.avro.util.Utf8, expected types Double, Float, Integer, Long"
          )
        }

        it("should decode Float as Double") {
          assertDecodeIs[Double](
            unsafeEncode(123f),
            Right(123d),
            Some(unsafeSchema[Float])
          )
        }

        it("should decode Int as Double") {
          assertDecodeIs[Double](
            unsafeEncode(123),
            Right(123d),
            Some(unsafeSchema[Int])
          )
        }

        it("should decode Long as Double") {
          assertDecodeIs[Double](
            unsafeEncode(123L),
            Right(123d),
            Some(unsafeSchema[Long])
          )
        }

        it("should decode Double as Double") {
          val value = 123d
          assertDecodeIs[Double](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("either") {
      describe("schema") {
        it("should be encoded as union") {
          assertSchemaIs[Either[Int, Double]] {
            """["int","double"]"""
          }
        }
      }

      describe("encode") {
        it("should encode left using left schema") {
          assertEncodeIs[Either[Int, Double]](
            Left(1),
            Right(1)
          )
        }

        it("should encode right using right schema") {
          assertEncodeIs[Either[Int, Double]](
            Right(1d),
            Right(1d)
          )
        }
      }

      describe("decode") {
        it("should error if schema type not in union") {
          assertDecodeError[Either[Int, Double]](
            unsafeEncode[Either[Int, Double]](Right(1d)),
            unsafeSchema[String],
            "Error decoding Either: Error decoding union: Exhausted alternatives for type java.lang.Double"
          )
        }

        it("should decode left value as left") {
          assertDecodeIs[Either[Int, Double]](
            unsafeEncode[Either[Int, Double]](Left(1)),
            Right(Left(1))
          )
        }

        it("should decode right value as right") {
          assertDecodeIs[Either[Int, Double]](
            unsafeEncode[Either[Int, Double]](Right(1d)),
            Right(Right(1d))
          )
        }
      }
    }

    describe("encode") {
      it("should encode using codec for type") {
        forAll { (n: Int) =>
          assert(Codec.encode(n).value == n)
        }
      }
    }

    describe("enum") {
      describe("schema") {
        it("should have expected schema") {
          assertSchemaIs[SealedTraitEnum] {
            """{"type":"enum","name":"SealedTraitEnum","namespace":"vulcan.examples","doc":"documentation","symbols":["first","second"],"default":"first","custom1":10,"custom2":"value2","aliases":["first","second"]}"""
          }
        }

        it("should capture errors on invalid schema") {
          assertSchemaError[`-SealedTraitEnumInvalidName`] {
            """org.apache.avro.SchemaParseException: Illegal initial character: -SealedTraitEnumInvalidName"""
          }
        }
      }

      describe("encode") {
        it("should error if returned string is not a schema symbol") {
          implicit val codec: Codec[SealedTraitEnum] =
            Codec.enumeration(
              name = "SealedTraitEnum",
              namespace = "vulcan.examples",
              symbols = List("symbol"),
              encode = _ => "not-symbol",
              decode = _ => Left(AvroError("error"))
            )

          assertEncodeError[SealedTraitEnum](
            FirstInSealedTraitEnum,
            "Error encoding vulcan.examples.SealedTraitEnum: Symbol not-symbol is not part of schema symbols [symbol]"
          )
        }

        it("should encode a valid symbol") {
          assertEncodeIs[SealedTraitEnum](
            FirstInSealedTraitEnum,
            Right(Avro.EnumSymbol(unsafeSchema[SealedTraitEnum], "first"))
          )
        }
      }

      describe("decode") {
        it("should error if schema type is not enum") {
          assertDecodeError[SealedTraitEnum](
            unsafeEncode[SealedTraitEnum](FirstInSealedTraitEnum),
            unsafeSchema[String],
            "Error decoding vulcan.examples.SealedTraitEnum: Got unexpected schema type STRING, expected schema type ENUM"
          )
        }

        it("should error if value is not GenericEnumSymbol") {
          assertDecodeError[SealedTraitEnum](
            unsafeEncode(10),
            unsafeSchema[SealedTraitEnum],
            "Error decoding vulcan.examples.SealedTraitEnum: Got unexpected type java.lang.Integer, expected type GenericEnumSymbol"
          )
        }

        it("should return default value if encoded value is not a schema symbol") {
          assertDecodeIs[SealedTraitEnum](
            Avro.EnumSymbol(
              SchemaBuilder
                .enumeration("vulcan.examples.SealedTraitEnum")
                .symbols("symbol"),
              "symbol"
            ),
            Right(FirstInSealedTraitEnum),
            Some(unsafeSchema[SealedTraitEnum])
          )
        }

        it("should error if encoded value is not a schema symbol and there is no default value") {
          assertDecodeError[SealedTraitEnumNoDefault](
            Avro.EnumSymbol(
              SchemaBuilder
                .enumeration("vulcan.examples.SealedTraitEnumNoDefault")
                .symbols("symbol"),
              "symbol"
            ),
            unsafeSchema[SealedTraitEnum],
            "Error decoding vulcan.examples.SealedTraitEnumNoDefault: symbol is not one of schema symbols [first, second]"
          )
        }

        it("should decode a valid symbol") {
          assertDecodeIs[SealedTraitEnum](
            unsafeEncode[SealedTraitEnum](FirstInSealedTraitEnum),
            Right(FirstInSealedTraitEnum)
          )
        }

        it("should decode a valid symbol with a different namespace") {
          assertDecodeIs[SealedTraitEnum](
            Avro.EnumSymbol(
              SchemaBuilder
                .enumeration("SealedTraitEnum")
                .namespace("vulcan.examples.alt") // The namespace is different
                .symbols("first", "second"),
              "second"
            ),
            Right(SecondInSealedTraitEnum),
            Some(unsafeSchema[SealedTraitEnum])
          )
        }
      }
    }

    describe("fixed") {
      describe("schema") {
        it("should have the expected schema") {
          assertSchemaIs[FixedBoolean] {
            """{"type":"fixed","name":"FixedBoolean","namespace":"vulcan.examples","doc":"A boolean represented as a byte","size":1,"custom":"value","aliases":["SomeOtherBoolean"]}"""
          }
        }

        it("should fail for negative size") {
          assert {
            Codec
              .fixed[Array[Byte]](
                name = "Name",
                size = -1,
                encode = bytes => bytes,
                decode = bytes => Right(bytes),
                namespace = ""
              )
              .schema
              .swap
              .value
              .message ==
              "org.apache.avro.AvroRuntimeException: Malformed data. Length is negative: -1"
          }
        }
      }

      describe("encode") {
        it("should error if length exceeds schema size") {
          assertEncodeError[FixedBoolean](
            TrueFixedBoolean,
            "Error encoding vulcan.examples.FixedBoolean: Got 2 bytes, expected maximum fixed size 1"
          )
        }

        it("should encode as fixed") {
          assertEncodeIs[FixedBoolean](
            FalseFixedBoolean,
            Right(GenericData.get().createFixed(null, Array(0.toByte), unsafeSchema[FixedBoolean]))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not fixed") {
          assertDecodeError[FixedBoolean](
            unsafeEncode[FixedBoolean](FalseFixedBoolean),
            unsafeSchema[String],
            "Error decoding vulcan.examples.FixedBoolean: Got unexpected schema type STRING, expected schema type FIXED"
          )
        }

        it("should error if value is not GenericFixed") {
          assertDecodeError[FixedBoolean](
            123,
            unsafeSchema[FixedBoolean],
            "Error decoding vulcan.examples.FixedBoolean: Got unexpected type java.lang.Integer, expected type GenericFixed"
          )
        }

        it("should error if length does not match schema size") {
          assertDecodeError[FixedBoolean](
            GenericData
              .get()
              .createFixed(
                null,
                Array(0.toByte, 1.toByte),
                SchemaBuilder
                  .builder("vulcan.examples")
                  .fixed("FixedBoolean")
                  .size(2)
              ),
            unsafeSchema[FixedBoolean],
            "Error decoding vulcan.examples.FixedBoolean: Got 2 bytes, expected fixed size 1"
          )
        }

        it("should decode as fixed") {
          assertDecodeIs[FixedBoolean](
            unsafeEncode[FixedBoolean](FalseFixedBoolean),
            Right(FalseFixedBoolean)
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
        it("should encode as float") {
          val value = 123f
          assertEncodeIs[Float](
            value,
            Right(value)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not float or promotable to float") {
          assertDecodeError[Float](
            unsafeEncode(123f),
            unsafeSchema[String],
            "Error decoding Float: Got unexpected schema type STRING, expected schema type FLOAT"
          )
        }

        it("should error if value is not float or promotable to float") {
          assertDecodeError[Float](
            unsafeEncode("foo"),
            unsafeSchema[Float],
            "Error decoding Float: Got unexpected type org.apache.avro.util.Utf8, expected type Float"
          )
        }

        it("should decode Int as Float") {
          assertDecodeIs[Float](
            unsafeEncode(123),
            Right(123f),
            Some(unsafeSchema[Int])
          )
        }

        it("should decode Long as Float") {
          assertDecodeIs[Float](
            unsafeEncode(123L),
            Right(123f),
            Some(unsafeSchema[Long])
          )
        }

        it("should decode Float as Float") {
          val value = 123f
          assertDecodeIs[Float](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("fromJson") {
      it("should decode from avro json format") {
        assert(Codec.fromJson[Int]("1", unsafeSchema[Int]) == Right(1))
      }

      it("should error if the json does not match the type") {
        val result = Codec.fromJson[Int]("badValue", unsafeSchema[String])
        assert(result.isLeft)
        assert(result.swap.exists(_.message.contains("Unrecognized token 'badValue'")))
      }

      it("should error if the schema does not match the type") {
        val result = Codec.fromJson[Int]("1", unsafeSchema[String])
        assert(result.isLeft)
        assert(
          result.swap.exists(
            _.message
              .contains("org.apache.avro.AvroTypeException: Expected string. Got VALUE_NUMBER_INT")
          )
        )
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
        it("should encode as long") {
          val value = {
            val instant = Instant.now()
            instant.minusNanos(instant.getNano().toLong)
          }

          assertEncodeIs[Instant](
            value,
            Right(value.toEpochMilli())
          )
        }
      }

      describe("decode") {

        it("should error if logical type is missing") {
          assertDecodeError[Instant](
            unsafeEncode(Instant.now()),
            unsafeSchema[Long],
            "Error decoding Instant: Got unexpected missing logical type"
          )
        }

        it("should error if logical type is not timestamp-millis") {
          assertDecodeError[Instant](
            unsafeEncode(Instant.now()), {
              LogicalTypes.timestampMicros().addToSchema {
                SchemaBuilder.builder().longType()
              }
            },
            "Error decoding Instant: Got unexpected logical type timestamp-micros"
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

    describe("imapError") {
      case class PosInt(value: Int)

      object PosInt {
        def apply(value: Int): Either[AvroError, PosInt] =
          if (value > 0) Right(new PosInt(value) {})
          else Left(AvroError(s"$value is not positive"))

        implicit val posIntCodec: Codec[PosInt] =
          Codec[Int].imapError(apply)(_.value)
      }

      describe("schema") {
        it("should use the schema of the underlying codec") {
          assertSchemaIs[PosInt] {
            """"int""""
          }
        }
      }

      describe("encode") {
        it("should encode using the underlying codec") {
          assertEncodeIs[PosInt](
            PosInt(1).value,
            Right(unsafeEncode(1))
          )
        }
      }

      describe("decode") {
        it("should succeed for valid values") {
          assertDecodeIs[PosInt](
            unsafeEncode(1),
            PosInt(1)
          )
        }

        it("should error for invalid values") {
          assertDecodeError[PosInt](
            unsafeEncode(0),
            unsafeSchema[PosInt],
            "0 is not positive"
          )
        }
      }
    }

    describe("imapTry") {
      case class PosInt(value: Int)

      object PosInt {
        def apply(value: Int): Try[PosInt] =
          if (value > 0) Success(new PosInt(value) {})
          else Failure(new RuntimeException(s"$value is not positive"))

        implicit val posIntCodec: Codec[PosInt] =
          Codec[Int].imapTry(apply)(_.value)
      }

      describe("schema") {
        it("should use the schema of the underlying codec") {
          assertSchemaIs[PosInt] {
            """"int""""
          }
        }
      }

      describe("encode") {
        it("should encode using the underlying codec") {
          assertEncodeIs[PosInt](
            PosInt(1).get,
            Right(unsafeEncode(1))
          )
        }
      }

      describe("decode") {
        it("should succeed for valid values") {
          assertDecodeIs[PosInt](
            unsafeEncode(1),
            PosInt(1).toEither.leftMap(AvroError.fromThrowable)
          )
        }

        it("should error for invalid values") {
          assertDecodeError[PosInt](
            unsafeEncode(0),
            unsafeSchema[PosInt],
            "java.lang.RuntimeException: 0 is not positive"
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
        it("should encode as int") {
          val value = 123
          assertEncodeIs[Int](
            value,
            Right(value)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int") {
          assertDecodeError[Int](
            unsafeEncode(123),
            unsafeSchema[Long],
            "Error decoding Int: Got unexpected schema type LONG, expected schema type INT"
          )
        }

        it("should error if value is not int") {
          assertDecodeError[Int](
            unsafeEncode(10L),
            unsafeSchema[Int],
            "Error decoding Int: Got unexpected type java.lang.Long, expected type Int"
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
            "Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[List[Int]](
            unsafeEncode(10),
            unsafeSchema[List[Int]],
            "Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
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
        it("should encode as int") {
          val value = LocalDate.now()
          assertEncodeIs[LocalDate](
            value,
            Right(value.toEpochDay().toInt)
          )
        }
      }

      describe("decode") {

        it("should error if logical type is not date") {
          assertDecodeError[LocalDate](
            unsafeEncode(LocalDate.now()),
            unsafeSchema[Int],
            "Error decoding LocalDate: Got unexpected missing logical type"
          )
        }

        it("should decode int as local date") {
          val value = LocalDate.now()
          assertDecodeIs[LocalDate](
            unsafeEncode(value),
            Right(value)
          )
        }

        it("should error if local date epoch days exceeds maximum integer size") {
          val value = LocalDate.MAX

          assertEncodeError[LocalDate](
            value,
            s"Error encoding LocalDate: Unable to encode date as epoch days of ${value.toEpochDay} exceeds the maximum integer size"
          )
        }
      }
    }

    describe("localTimeMillis") {
      implicit val codec: Codec[LocalTime] = Codec.localTimeMillis
      describe("schema") {
        it("should be encoded as int with logical type time-millis") {
          assertSchemaIs[LocalTime] {
            """{"type":"int","logicalType":"time-millis"}"""
          }
        }
      }

      describe("encode") {
        it("should encode as int") {
          val value = LocalTime.now()
          assertEncodeIs[LocalTime](
            value,
            Right(
              java.lang.Integer.valueOf(TimeUnit.NANOSECONDS.toMillis(value.toNanoOfDay()).toInt)
            )
          )
        }
      }

      describe("decode") {

        it("should error if logical type is not time-millis") {
          assertDecodeError[LocalTime](
            unsafeEncode(LocalTime.now()),
            unsafeSchema[Int],
            "Error decoding LocalTime: Got unexpected missing logical type"
          )
        }

        it("should decode int as local time-millis") {
          val value = LocalTime.now()
          assertDecodeIs[LocalTime](
            unsafeEncode(value),
            Right(value.truncatedTo(ChronoUnit.MILLIS))
          )
        }
      }
    }

    describe("localTimeMicros") {
      implicit val codec: Codec[LocalTime] = Codec.localTimeMicros
      describe("schema") {
        it("should be encoded as int with logical type time-micros") {
          assertSchemaIs[LocalTime] {
            """{"type":"long","logicalType":"time-micros"}"""
          }
        }
      }

      describe("encode") {
        it("should encode as long") {
          val value = LocalTime.now()
          assertEncodeIs[LocalTime](
            value,
            Right(java.lang.Long.valueOf(TimeUnit.NANOSECONDS.toMicros(value.toNanoOfDay())))
          )
        }
      }

      describe("decode") {

        it("should error if logical type is not time-micros") {
          assertDecodeError[LocalTime](
            unsafeEncode(LocalTime.now()),
            unsafeSchema[Long],
            "Error decoding LocalTime: Got unexpected missing logical type"
          )
        }

        it("should decode long as local time-micros") {
          val value = LocalTime.now()
          assertDecodeIs[LocalTime](
            unsafeEncode(value),
            Right(value.truncatedTo(ChronoUnit.MICROS))
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
        it("should encode as long") {
          val value = 123L
          assertEncodeIs[Long](
            value,
            Right(value)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int or long") {
          assertDecodeError[Long](
            unsafeEncode(123L),
            unsafeSchema[String],
            "Error decoding Long: Got unexpected schema type STRING, expected schema type LONG"
          )
        }

        it("should error if value is not int or long") {
          assertDecodeError[Long](
            unsafeEncode(123.0),
            unsafeSchema[Long],
            "Error decoding Long: Got unexpected type java.lang.Double, expected type Long"
          )
        }

        it("should decode int as long") {
          assertDecodeIs[Long](
            unsafeEncode(123),
            Right(123L),
            Some(unsafeSchema[Int])
          )
        }

        it("should decode long as long") {
          val value = 123L
          assertDecodeIs[Long](
            unsafeEncode(value),
            Right(value)
          )
        }
      }
    }

    describe("map") {
      describe("schema") {
        it("should be encoded as map") {
          assertSchemaIs[Map[String, Int]] {
            """{"type":"map","values":"int"}"""
          }
        }
      }

      describe("encode") {
        it("should encode as java map using encoder for value") {
          assertEncodeIs[Map[String, Int]](
            Map("key" -> 1),
            Right(Map(Avro.String("key") -> 1).asJava)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not map") {
          assertDecodeError[Map[String, Int]](
            unsafeEncode[Map[String, Int]](Map("key" -> 1)),
            SchemaBuilder.builder().intType(),
            "Error decoding Map: Got unexpected schema type INT, expected schema type MAP"
          )
        }

        it("should error if value is not java.util.Map") {
          assertDecodeError[Map[String, Int]](
            123,
            unsafeSchema[Map[String, Int]],
            "Error decoding Map: Got unexpected type java.lang.Integer, expected type java.util.Map"
          )
        }

        it("should error if keys are not strings") {
          assertDecodeError[Map[String, Int]](
            Map(1 -> 2).asJava,
            unsafeSchema[Map[String, Int]],
            "Error decoding Map: Got unexpected map key with type java.lang.Integer, expected Utf8"
          )
        }

        it("should error if any keys are null") {
          assertDecodeError[Map[String, Int]](
            Map((null, 2)).asJava,
            unsafeSchema[Map[String, Int]],
            "Error decoding Map: Got unexpected map key with type null, expected Utf8"
          )
        }

        it("should decode to map using decoder for value") {
          assertDecodeIs[Map[String, Int]](
            unsafeEncode[Map[String, Int]](Map("key" -> 1)),
            Right(Map("key" -> 1))
          )
        }
      }
    }

    describe("none") {
      describe("schema") {
        it("should be encoded as null") {
          assertSchemaIs[None.type] {
            """"null""""
          }
        }
      }

      describe("encode") {
        it("should encode as null") {
          assertEncodeIs[None.type](
            None,
            Right(null)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not null") {
          assertDecodeError[None.type](
            unsafeEncode(None),
            unsafeSchema[Int],
            "Error decoding None: Got unexpected schema type INT, expected schema type NULL"
          )
        }

        it("should error if value is not null") {
          assertDecodeError[None.type](
            unsafeEncode(10),
            unsafeSchema[None.type],
            "Error decoding None: Got unexpected type java.lang.Integer, expected type null"
          )
        }

        it("should decode null as None") {
          assertDecodeIs[None.type](
            unsafeEncode(()),
            Right(None)
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
            "Error decoding NonEmptyChain: Error decoding Chain: Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyChain[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyChain[Int]],
            "Error decoding NonEmptyChain: Error decoding Chain: Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyChain[Int]](
            unsafeEncode(Chain.empty[Int]),
            unsafeSchema[NonEmptyChain[Int]],
            "Error decoding NonEmptyChain: Got unexpected empty collection"
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
            "Error decoding NonEmptyList: Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyList[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyList[Int]],
            "Error decoding NonEmptyList: Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyList[Int]](
            unsafeEncode(List.empty[Int]),
            unsafeSchema[NonEmptyList[Int]],
            "Error decoding NonEmptyList: Got unexpected empty collection"
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
            "Error decoding NonEmptySet: Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptySet[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptySet[Int]],
            "Error decoding NonEmptySet: Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptySet[Int]](
            unsafeEncode(Set.empty[Int]),
            unsafeSchema[NonEmptySet[Int]],
            "Error decoding NonEmptySet: Got unexpected empty collection"
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
            "Error decoding NonEmptyVector: Error decoding Vector: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[NonEmptyVector[Int]](
            unsafeEncode(10),
            unsafeSchema[NonEmptyVector[Int]],
            "Error decoding NonEmptyVector: Error decoding Vector: Got unexpected type java.lang.Integer, expected type Collection"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyVector[Int]](
            unsafeEncode(Vector.empty[Int]),
            unsafeSchema[NonEmptyVector[Int]],
            "Error decoding NonEmptyVector: Got unexpected empty collection"
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

    describe("nonEmptyMap") {
      describe("schema") {
        it("should be encoded as map") {
          assertSchemaIs[NonEmptyMap[String, Int]] {
            """{"type":"map","values":"int"}"""
          }
        }
      }

      describe("encode") {
        it("should encode as java map using encoder for value") {
          assertEncodeIs[NonEmptyMap[String, Int]](
            NonEmptyMap.of("key1" -> 1, "key2" -> 2, "key3" -> 3),
            Right(
              Map(Avro.String("key1") -> 1, Avro.String("key2") -> 2, Avro.String("key3") -> 3).asJava
            )
          )
        }
      }

      describe("decode") {
        it("should error if schema is not map") {
          assertDecodeError[NonEmptyMap[String, Int]](
            unsafeEncode[NonEmptyMap[String, Int]](NonEmptyMap.one("key", 1)),
            SchemaBuilder.builder().intType(),
            "Error decoding NonEmptyMap: Error decoding Map: Got unexpected schema type INT, expected schema type MAP"
          )
        }

        it("should error if value is not java.util.Map") {
          assertDecodeError[NonEmptyMap[String, Int]](
            123,
            unsafeSchema[NonEmptyMap[String, Int]],
            "Error decoding NonEmptyMap: Error decoding Map: Got unexpected type java.lang.Integer, expected type java.util.Map"
          )
        }

        it("should error if keys are not strings") {
          assertDecodeError[NonEmptyMap[String, Int]](
            NonEmptyMap.one(1, 2).toSortedMap.asJava,
            unsafeSchema[NonEmptyMap[String, Int]],
            "Error decoding NonEmptyMap: Error decoding Map: Got unexpected map key with type java.lang.Integer, expected Utf8"
          )
        }

        it("should error if any keys are null") {
          assertDecodeError[NonEmptyMap[String, Int]](
            Map((null, 2)).asJava,
            unsafeSchema[NonEmptyMap[String, Int]],
            "Error decoding NonEmptyMap: Error decoding Map: Got unexpected map key with type null, expected Utf8"
          )
        }

        it("should error on empty collection") {
          assertDecodeError[NonEmptyMap[String, Int]](
            unsafeEncode(Map.empty[String, Int]),
            unsafeSchema[NonEmptyMap[String, Int]],
            "Error decoding NonEmptyMap: Got unexpected empty collection"
          )
        }

        it("should decode to map using decoder for value") {
          val value = NonEmptyMap.of("key1" -> 1, "key2" -> 2, "key3" -> 3)
          assertDecodeIs[NonEmptyMap[String, Int]](
            unsafeEncode[NonEmptyMap[String, Int]](value),
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

        it("should capture errors on nested unions") {
          assertSchemaError[Option[Option[Int]]] {
            """org.apache.avro.AvroRuntimeException: Duplicate in union:null"""
          }
        }
      }

      describe("encode") {
        it("should encode Some") {
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
        it("should error if schema is not in union") {
          assertDecodeError[Option[Int]](
            unsafeEncode(Option(1)),
            unsafeSchema[String],
            "Error decoding Option: Error decoding Int: Got unexpected schema type STRING, expected schema type INT"
          )
        }

        it("should decode if schema is part of union") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(unsafeSchema[Int])
          )
        }

        it("should decode even if there is one schema in union") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(SchemaBuilder.unionOf().intType().endUnion())
          )
        }

        it("should decode even if there is not null in union") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(SchemaBuilder.unionOf().intType().and().stringType().endUnion())
          )
        }

        it("should decode if there are more than two schemas in union") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(SchemaBuilder.unionOf().intType().and().stringType().and().nullType().endUnion())
          )
        }

        it("should support null as first schema type in union") {

          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(
              SchemaBuilder
                .unionOf()
                .nullType()
                .and()
                .intType()
                .endUnion()
            )
          )
        }

        it("should support null as second schema type in union") {
          assertDecodeIs[Option[Int]](
            unsafeEncode(Option(1)),
            Right(Some(1)),
            Some(
              SchemaBuilder
                .unionOf()
                .intType()
                .and()
                .nullType()
                .endUnion()
            )
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

    describe("option.union") {
      sealed trait FirstOrSecond

      final case class First(value: Int) extends FirstOrSecond
      object First {
        implicit val firstCodec: Codec[First] =
          Codec[Int].imap(apply)(_.value)
      }

      final case class Second(value: Double) extends FirstOrSecond
      object Second {
        implicit val secondCodec: Codec[Second] =
          Codec[Double].imap(apply)(_.value)
      }

      implicit val codec: Codec[Option[FirstOrSecond]] =
        Codec.union(alt => alt[None.type] |+| alt[Some[First]] |+| alt[Some[Second]])

      describe("schema") {
        it("should be encoded as union") {
          assertSchemaIs[Option[FirstOrSecond]] {
            """["null","int","double"]"""
          }
        }
      }

      describe("encode") {
        it("should encode none as null") {
          assert(codec.encode(None) == Right(null))
        }

        it("should encode first as int") {
          forAll { (n: Int) =>
            assert(codec.encode(Some(First(n))) == Right(n))
          }
        }

        it("should encode second as double") {
          forAll { (n: Double) =>
            assert(codec.encode(Some(Second(n))) == Right(n))
          }
        }
      }

      describe("decode") {
        it("should decode null as none") {
          assert(codec.decode(null, unsafeSchema[Option[FirstOrSecond]]) == Right(None))
        }

        it("should decode int as first") {
          forAll { (n: Int) =>
            assert(codec.decode(n, unsafeSchema[Option[FirstOrSecond]]) == Right(Some(First(n))))
          }
        }

        it("should decode double as second") {
          forAll { (n: Double) =>
            assert(
              codec.decode(n, unsafeSchema[Option[FirstOrSecond]]) == Right(Some(Second(n)))
            )
          }
        }
      }
    }

    describe("record") {
      describe("schema") {
        it("should have the expected schema") {
          assert {
            Codec[CaseClassTwoFields].schema.value.toString() ==
              """{"type":"record","name":"CaseClassTwoFields","namespace":"vulcan.examples","doc":"some documentation for example","fields":[{"name":"name","type":"string","doc":"some doc","default":"default name","order":"descending","aliases":["TheAlias"],"custom":"value"},{"name":"age","type":"int"}],"custom":[1,2,3],"aliases":["FirstAlias","SecondAlias"]}"""
          }
        }

        it("should error if default value can't be encoded") {
          implicit val intCodec: Codec[Int] =
            Codec.int.imapErrors(_ => Left(AvroError("error")))(_ => Left(AvroError("error")))

          implicit val caseClassFieldCodec: Codec[CaseClassField] =
            Codec.record[CaseClassField]("CaseClassField", "") { field =>
              field("value", _.value, default = Some(10)).map(CaseClassField(_))
            }

          assert(caseClassFieldCodec.schema.swap.value.message == "error")
        }

        it("should support None as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(None)).map(Test(_))
            }

          assertSchemaIs[Test] {
            """{"type":"record","name":"Test","fields":[{"name":"value","type":["null","int"],"default":null}]}"""
          }
        }

        it("should support Some as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(Some(0)))(
                Codec.union(alt => alt[Some[Int]] |+| alt[None.type])
              ).map(Test(_))
            }

          assertSchemaIs[Test] {
            """{"type":"record","name":"Test","fields":[{"name":"value","type":["int","null"],"default":0}]}"""
          }
        }

        describe("default") {
          it("should support null default value") {
            case class Test(value: Unit)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(())).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"null","default":null}]}"""
            }
          }

          it("should support boolean default value") {
            case class Test(value: Boolean)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(false)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"boolean","default":false}]}"""
            }
          }

          it("should support int default value") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(0)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","default":0}]}"""
            }
          }

          it("should support long default value") {
            case class Test(value: Long)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(0L)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"long","default":0}]}"""
            }
          }

          it("should support float default value") {
            case class Test(value: Float)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(0.0f)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"float","default":0.0}]}"""
            }
          }

          it("should support double default value") {
            case class Test(value: Double)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(0.0d)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"double","default":0.0}]}"""
            }
          }

          it("should support bytes default value") {
            case class Test(value: Array[Byte])

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(Array[Byte](Byte.MinValue, Byte.MaxValue)))
                  .map(Test(_))
              }

            val expectedDefault = "\u0080\u007f"

            assertSchemaIs[Test] {
              s"""{"type":"record","name":"Test","fields":[{"name":"value","type":"bytes","default":"$expectedDefault"}]}"""
            }
          }

          it("should support string default value") {
            case class Test(value: String)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some("default-value")).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"string","default":"default-value"}]}"""
            }
          }

          it("should support record default value") {
            sealed trait CustomEnum
            case object First extends CustomEnum
            case object Second extends CustomEnum

            implicit val customEnumCodec: Codec[CustomEnum] =
              Codec.enumeration(
                name = "CustomEnum",
                symbols = List("first", "second"),
                encode = {
                  case First  => "first"
                  case Second => "second"
                },
                decode = {
                  case "first"  => Right(First)
                  case "second" => Right(Second)
                  case other    => Left(AvroError(other))
                },
                namespace = ""
              )

            case class Inner(value: Int, customEnum: CustomEnum)

            implicit val innerCodec: Codec[Inner] =
              Codec.record("Inner", "") { field =>
                (
                  field("value", _.value),
                  field("customEnum", _.customEnum, default = Some(First))
                ).mapN(Inner(_, _))
              }

            case class Test(value: Inner)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(Inner(0, Second))).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"record","name":"Inner","fields":[{"name":"value","type":"int"},{"name":"customEnum","type":{"type":"enum","name":"CustomEnum","symbols":["first","second"]},"default":"first"}]},"default":{"value":0,"customEnum":"second"}}]}"""
            }
          }

          it("should support enum default value") {
            sealed trait CustomEnum
            case object First extends CustomEnum
            case object Second extends CustomEnum

            implicit val customEnumCodec: Codec[CustomEnum] =
              Codec.enumeration(
                name = "CustomEnum",
                symbols = List("first", "second"),
                encode = {
                  case First  => "first"
                  case Second => "second"
                },
                decode = {
                  case "first"  => Right(First)
                  case "second" => Right(Second)
                  case other    => Left(AvroError(other))
                },
                namespace = ""
              )

            case class Test(value: CustomEnum)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(First)).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"enum","name":"CustomEnum","symbols":["first","second"]},"default":"first"}]}"""
            }
          }

          it("should support array default value") {
            case class Test(value: List[Int])

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(List(123, 456))).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"array","items":"int"},"default":[123,456]}]}"""
            }
          }

          it("should support record array default value") {
            case class Test(value: List[Element])
            case class Element(value: Int)

            implicit val elementCodec: Codec[Element] =
              Codec.record("Element", "") { field =>
                field("value", _.value).map(Element(_))
              }
            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(List(Element(123), Element(456)))).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"array","items":{"type":"record","name":"Element","fields":[{"name":"value","type":"int"}]}},"default":[{"value":123},{"value":456}]}]}"""
            }
          }

          it("should support map default value") {
            case class Test(value: Map[String, Int])

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(Map("key" -> 0))).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"map","values":"int"},"default":{"key":0}}]}"""
            }
          }

          it("should support record map default value") {
            case class Test(value: Map[String, Value])
            case class Value(value: Int)

            implicit val valueCodec: Codec[Value] =
              Codec.record("Value", "") { field =>
                field("value", _.value).map(Value(_))
              }
            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(Map("key" -> Value(0)))).map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"map","values":{"type":"record","name":"Value","fields":[{"name":"value","type":"int"}]}},"default":{"key":{"value":0}}}]}"""
            }
          }

          it("should support fixed default value") {
            case class Inner(value: Array[Byte])

            implicit val innerCodec: Codec[Inner] =
              Codec.fixed(
                name = "Inner",
                size = 1,
                encode = _.value,
                decode = bytes => Right(Inner(bytes)),
                namespace = ""
              )

            case class Test(value: Inner)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, default = Some(Inner(Array[Byte](Byte.MaxValue))))
                  .map(Test(_))
              }

            val expectedDefault = "\u007f"

            assertSchemaIs[Test] {
              s"""{"type":"record","name":"Test","fields":[{"name":"value","type":{"type":"fixed","name":"Inner","size":1},"default":"$expectedDefault"}]}"""
            }
          }
        }

        describe("props") {
          it("should support boolean custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", true))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":true}]}"""
            }
          }

          it("should support int custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", 123))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":123}]}"""
            }
          }

          it("should support long custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", 123L))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":123}]}"""
            }
          }

          it("should support float custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", 123.0f))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":123.0}]}"""
            }
          }

          it("should support double custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", 123.0d))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":123.0}]}"""
            }
          }

          it("should support bytes custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", Array[Byte](Byte.MaxValue)))
                  .map(Test(_))
              }

            val expectedCustom = "\u007f"

            assertSchemaIs[Test] {
              s"""{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":"$expectedCustom"}]}"""
            }
          }

          it("should support string custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", "value"))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":"value"}]}"""
            }
          }

          it("should support record custom property") {
            case class Record(value: String)

            implicit val recordCodec: Codec[Record] =
              Codec.record("Record", "") { field =>
                field("value", _.value).map(Record(_))
              }

            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", Record("some-value")))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":{"value":"some-value"}}]}"""
            }
          }

          it("should support enum custom property") {
            sealed trait CustomEnum
            case object First extends CustomEnum
            case object Second extends CustomEnum

            implicit val customEnumCodec: Codec[CustomEnum] =
              Codec.enumeration(
                name = "CustomEnum",
                symbols = List("first", "second"),
                encode = {
                  case First  => "first"
                  case Second => "second"
                },
                decode = {
                  case "first"  => Right(First)
                  case "second" => Right(Second)
                  case other    => Left(AvroError(other))
                },
                namespace = ""
              )

            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", (First: CustomEnum)))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":"first"}]}"""
            }
          }

          it("should support array custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", List(123, 456)))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":[123,456]}]}"""
            }
          }

          it("should support map custom property") {
            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field("value", _.value, props = Props.one("custom", Map("key" -> 1)))
                  .map(Test(_))
              }

            assertSchemaIs[Test] {
              """{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":{"key":1}}]}"""
            }
          }

          it("should support fixed custom property") {
            case class Fixed(value: Array[Byte])

            implicit val fixedCodec: Codec[Fixed] =
              Codec.fixed(
                name = "Fixed",
                size = 1,
                encode = _.value,
                decode = bytes => Right(Fixed(bytes)),
                namespace = ""
              )

            case class Test(value: Int)

            implicit val testCodec: Codec[Test] =
              Codec.record("Test", "") { field =>
                field(
                  "value",
                  _.value,
                  props = Props.one("custom", Fixed(Array[Byte](Byte.MaxValue)))
                ).map(Test(_))
              }

            val expectedCustom = "\u007f"

            assertSchemaIs[Test] {
              s"""{"type":"record","name":"Test","fields":[{"name":"value","type":"int","custom":"$expectedCustom"}]}"""
            }
          }
        }
      }

      describe("encode") {
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

        it("should support None as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(None)).map(Test(_))
            }

          assertEncodeIs[Test](
            Test(None),
            Right {
              val record = new GenericData.Record(unsafeSchema[Test])
              record.put(0, null)
              record
            }
          )
        }

        it("should support Some as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(Some(0)))(
                Codec.union(alt => alt[Some[Int]] |+| alt[None.type])
              ).map(Test(_))
            }

          assertEncodeIs[Test](
            Test(None),
            Right {
              val record = new GenericData.Record(unsafeSchema[Test])
              record.put(0, null)
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
            "Error decoding vulcan.examples.CaseClassTwoFields: Got unexpected schema type STRING, expected schema type RECORD"
          )
        }

        it("should error if value is not indexed record") {
          assertDecodeError[CaseClassTwoFields](
            unsafeEncode(123),
            unsafeSchema[CaseClassTwoFields],
            "Error decoding vulcan.examples.CaseClassTwoFields: Got unexpected type java.lang.Integer, expected type IndexedRecord"
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
            "Error decoding vulcan.examples.CaseClassTwoFields: Record writer schema is missing field 'age'"
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

        it("should support None as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(None)).map(Test(_))
            }

          assertDecodeIs[Test](
            unsafeEncode(Test(None)),
            Right(Test(None))
          )
        }

        it("should support Some as default value") {
          case class Test(value: Option[Int])

          implicit val testCodec: Codec[Test] =
            Codec.record("Test", "") { field =>
              field("value", _.value, default = Some(Some(0)))(
                Codec.union(alt => alt[Some[Int]] |+| alt[None.type])
              ).map(Test(_))
            }

          assertDecodeIs[Test](
            unsafeEncode(Test(None)),
            Right(Test(None))
          )
        }

        it("should decode field with aliased name") {
          case class Aliased(aliasedField: Int)
          implicit val codec: Codec[Aliased] =
            Codec.record("CaseClassField", "") { field =>
              field("aliasedField", _.aliasedField, aliases = Seq("value")).map(Aliased(_))
            }

          assertDecodeIs[Aliased](
            unsafeEncode(CaseClassField(3)),
            Right(Aliased(3))
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
            "Error decoding Seq: Error decoding List: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Seq[Int]](
            unsafeEncode(10),
            unsafeSchema[Seq[Int]],
            "Error decoding Seq: Error decoding List: Got unexpected type java.lang.Integer, expected type Collection"
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
          Codec[Option[Option[Int]]].show == """AvroError(org.apache.avro.AvroRuntimeException: Duplicate in union:null)"""
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
            "Error decoding Set: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Set[Int]](
            unsafeEncode(10),
            unsafeSchema[Set[Int]],
            "Error decoding Set: Got unexpected type java.lang.Integer, expected type Collection"
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

    describe("short") {
      describe("schema") {
        it("should be encoded as int") {
          assertSchemaIs[Short] {
            """"int""""
          }
        }
      }

      describe("encode") {
        it("should encode as int") {
          val value = 1.toShort
          assertEncodeIs[Short](
            value,
            Right(1)
          )
        }
      }

      describe("decode") {
        it("should error if schema is not int") {
          assertDecodeError[Short](
            unsafeEncode(1.toShort),
            unsafeSchema[String],
            "Error decoding Short: Error decoding Int: Got unexpected schema type STRING, expected schema type INT"
          )
        }

        it("should error if value is not int") {
          assertDecodeError[Short](
            unsafeEncode("value"),
            unsafeSchema[Short],
            "Error decoding Short: Error decoding Int: Got unexpected type org.apache.avro.util.Utf8, expected type Int"
          )
        }

        it("should error if int value is not short") {
          val gen =
            Gen.oneOf(
              Gen.chooseNum(Int.MinValue, Short.MinValue.toInt - 1),
              Gen.chooseNum(Short.MaxValue.toInt + 1, Int.MaxValue)
            )

          forAll(gen) { nonShort =>
            assertDecodeError[Short](
              unsafeEncode(nonShort),
              unsafeSchema[Short],
              s"Error decoding Short: Got unexpected Int value $nonShort, expected value in range -32768 to 32767"
            )
          }
        }

        it("should decode as short") {
          forAll { (short: Short) =>
            assertDecodeIs[Short](
              unsafeEncode(short),
              Right(short)
            )
          }
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
        it("should encode as utf8") {
          val value = "abc"
          assertEncodeIs[String](
            value,
            Right(Avro.String(value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not string or bytes") {
          assertDecodeError[String](
            unsafeEncode("abc"),
            unsafeSchema[Int],
            "Error decoding String: Got unexpected schema type INT, expected schema type STRING"
          )
        }

        it("should error if value is not utf8, string, or bytes") {
          assertDecodeError[String](
            unsafeEncode(10),
            unsafeSchema[String],
            "Error decoding String: Got unexpected type java.lang.Integer, expected types String, Utf8"
          )
        }

        it("should decode utf8 as string") {
          val value = "abc"
          assertDecodeIs[String](
            unsafeEncode(value),
            Right(value)
          )
        }

        it("should decode string as string") {
          val value = "abc"
          assertDecodeIs[String](
            value,
            Right(value)
          )
        }

        it("should decode bytes as string") {
          val value = ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8))
          assertDecodeIs[String](
            value,
            Right("abc"),
            Some(SchemaBuilder.builder().bytesType())
          )
        }
      }
    }

    describe("toJson") {
      it("should encode to Json format") {
        assert(Codec.toJson[Int](1) == Right("1"))
      }
    }

    describe("union") {
      describe("schema") {
        it("should encode as union") {
          assertSchemaIs[SealedTraitCaseClass] {
            """[{"type":"record","name":"FirstInSealedTraitCaseClass","namespace":"com.example","fields":[{"name":"value","type":"int"}]},{"type":"record","name":"SecondInSealedTraitCaseClass","namespace":"com.example","fields":[{"name":"value","type":"string"}]},{"type":"array","items":"int"}]"""
          }
        }

        it("should capture errors on nested unions") {
          assertSchemaError[SealedTraitCaseClassNestedUnion] {
            """org.apache.avro.AvroRuntimeException: Nested union: [["null","int"]]"""
          }
        }
      }

      describe("encode") {
        it("should error if subtype is not an alternative") {
          assertEncodeError[SealedTraitCaseClassIncomplete](
            SecondInSealedTraitCaseClassIncomplete(0d),
            "Error encoding union: Exhausted alternatives for type vulcan.examples.SecondInSealedTraitCaseClassIncomplete"
          )
        }

        it("should error if subtype is not an alternative and value null") {
          assertEncodeError[SealedTraitCaseClassIncomplete](
            null,
            "Error encoding union: Exhausted alternatives for type null"
          )
        }

        it("should encode with encoder for alternative") {
          val value = FirstInSealedTraitCaseClass(0)
          assertEncodeIs[SealedTraitCaseClass](
            value,
            Right(unsafeEncode[SealedTraitCaseClass](value))
          )
        }
      }

      describe("decode") {
        it("should error if schema is not in union") {
          assertDecodeError[SealedTraitCaseClass](
            unsafeEncode[SealedTraitCaseClass](FirstInSealedTraitCaseClass(0)),
            unsafeSchema[String],
            "Error decoding union: Missing schema FirstInSealedTraitCaseClass in union"
          )
        }

        it("should decode if schema is part of union") {
          assertDecodeIs[SealedTraitCaseClass](
            unsafeEncode[SealedTraitCaseClass](FirstInSealedTraitCaseClass(0)),
            Right(FirstInSealedTraitCaseClass(0)),
            Some(unsafeSchema[FirstInSealedTraitCaseClass])
          )
        }

        it("should decode if schema is union with records of the same name") {
          assertDecodeIs[SealedTraitCaseClassSharedName](
            unsafeEncode[SealedTraitCaseClassSharedName](
              Second.SharedNameSealedTraitCaseClass("hello")
            ),
            Right(Second.SharedNameSealedTraitCaseClass("hello")),
            Some(unsafeSchema[Second.SharedNameSealedTraitCaseClass])
          )
        }

        it("should error if value is not an alternative") {
          assertDecodeError[SealedTraitCaseClass](
            unsafeEncode(123d),
            unsafeSchema[SealedTraitCaseClass],
            "Error decoding union: Exhausted alternatives for type java.lang.Double"
          )
        }

        it("should error if value is null and not an alternative") {
          assertDecodeError[SealedTraitCaseClass](
            null,
            unsafeSchema[SealedTraitCaseClass],
            "Error decoding union: Exhausted alternatives for type null"
          )
        }

        it("should error if no schema in union with container name") {
          assertDecodeError[SealedTraitCaseClassSingle](
            unsafeEncode[SealedTraitCaseClassSingle](CaseClassInSealedTraitCaseClassSingle(0)),
            unsafeSchema[SealedTraitCaseClass],
            "Error decoding union: Missing schema CaseClassInSealedTraitCaseClassSingle in union"
          )
        }

        it("should error if no alternative with container name") {
          assertDecodeError[SealedTraitCaseClass](
            unsafeEncode[SealedTraitCaseClassSingle](CaseClassInSealedTraitCaseClassSingle(0)),
            unsafeSchema[SealedTraitCaseClassSingle],
            "Error decoding union: Missing alternative CaseClassInSealedTraitCaseClassSingle in union"
          )
        }

        it("should decode using schema and decoder for alternative") {
          assertDecodeIs[SealedTraitCaseClass](
            unsafeEncode[SealedTraitCaseClass](FirstInSealedTraitCaseClass(0)),
            Right(FirstInSealedTraitCaseClass(0))
          )
        }

        it("should decode using schema with aliased name") {

          implicit val secondCodec: Codec[SecondInSealedTraitCaseClass] =
            Codec.record(
              name = "AliasedInSealedTraitCaseClass",
              namespace = "com.example",
              aliases = Seq("SecondInSealedTraitCaseClass")
            ) { field =>
              field("value", _.value).map(SecondInSealedTraitCaseClass(_))
            }

          implicit val codec: Codec[SealedTraitCaseClass] = Codec.union(
            alt =>
              alt[FirstInSealedTraitCaseClass]
                |+| alt[SecondInSealedTraitCaseClass]
                |+| alt[ThirdInSealedTraitCaseClass]
          )

          assertDecodeIs[SealedTraitCaseClass](
            unsafeEncode[SealedTraitCaseClass](SecondInSealedTraitCaseClass("foo"))(
              SealedTraitCaseClass.sealedTraitCaseClassCodec
            ),
            Right(SecondInSealedTraitCaseClass("foo")),
            Some(SealedTraitCaseClass.sealedTraitCaseClassCodec.schema.value)
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
            "Error decoding Unit: Got unexpected schema type INT, expected schema type NULL"
          )
        }

        it("should error if value is not null") {
          assertDecodeError[Unit](
            unsafeEncode(10),
            unsafeSchema[Unit],
            "Error decoding Unit: Got unexpected type java.lang.Integer, expected type null"
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
        it("should encode as utf8") {
          val value = UUID.randomUUID()
          assertEncodeIs[UUID](
            value,
            Right(Avro.String(value.toString()))
          )
        }
      }

      describe("decode") {

        it("should error if logical type is not uuid") {
          assertDecodeError[UUID](
            unsafeEncode(UUID.randomUUID()),
            unsafeSchema[String],
            "Error decoding UUID: Got unexpected missing logical type"
          )
        }

        it("should error if value is not uuid") {
          assertDecodeError[UUID](
            Avro.String("not-uuid"),
            unsafeSchema[UUID],
            "Error decoding UUID: java.lang.IllegalArgumentException: Invalid UUID string: not-uuid"
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
            "Error decoding Vector: Got unexpected schema type INT, expected schema type ARRAY"
          )
        }

        it("should error if value is not collection") {
          assertDecodeError[Vector[Int]](
            unsafeEncode(10),
            unsafeSchema[Vector[Int]],
            "Error decoding Vector: Got unexpected type java.lang.Integer, expected type Collection"
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
}
trait CodecSpecHelpers {
  self: BaseSpec =>
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
    assert {
      val encode = codec.encode(a).value
      encode === encoded.value
    }

  def assertDecodeIs[A](
    value: Any,
    decoded: Either[AvroError, A],
    schema: Option[Schema] = None
  )(implicit codec: Codec[A]): Assertion =
    assert {
      val decode =
        schema
          .map(codec.decode(value, _).value)
          .getOrElse(unsafeDecode(value))

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
