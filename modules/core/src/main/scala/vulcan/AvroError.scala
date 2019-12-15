/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.{Eq, Show}
import cats.data.NonEmptyList
import cats.instances.string._
import org.apache.avro.{Schema, LogicalType}
import scala.util.control.NonFatal

/**
  * Error which occurred while generating a schema, or
  * while encoding or decoding.
  *
  * Use [[AvroError.apply]] to create an instance, and
  * [[AvroError#message]] to retrieve the message.
  */
sealed abstract class AvroError {
  def message: String

  def throwable: Throwable
}

final object AvroError {
  final def apply(message: => String): AvroError = {
    val _message = () => message

    new AvroError {
      override final def message: String =
        _message()

      override final def throwable: Throwable =
        AvroException(message)

      override final def toString: String =
        s"AvroError($message)"
    }
  }

  implicit final val avroErrorShow: Show[AvroError] =
    Show.fromToString

  implicit final val avroErrorEq: Eq[AvroError] =
    Eq.by(_.message)

  private[vulcan] final def catchNonFatal[A](
    either: => Either[AvroError, A]
  ): Either[AvroError, A] =
    try {
      either
    } catch {
      case e if NonFatal(e) =>
        Left(AvroError.fromThrowable(e))
    }

  private[vulcan] final def decodeDecimalPrecisionExceeded(
    actualPrecision: Int,
    expectedPrecision: Int
  ): AvroError =
    AvroError {
      s"Unable to decode decimal with precision $actualPrecision exceeding schema precision $expectedPrecision"
    }

  private[vulcan] final def decodeEmptyCollection(decodingTypeName: String): AvroError =
    AvroError(s"Got unexpected empty collection while decoding $decodingTypeName")

  private[vulcan] final def decodeNotEqualFixedSize(
    length: Int,
    fixedSize: Int,
    decodingTypeName: String
  ): AvroError =
    AvroError(s"Got $length bytes while decoding $decodingTypeName, expected fixed size $fixedSize")

  private[vulcan] final def decodeMissingRecordField(
    fieldName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      s"Record writer schema is missing field '$fieldName' while decoding $decodingTypeName"
    }

  private[vulcan] final def decodeMissingUnionSchema(
    subtypeName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError(s"Missing schema $subtypeName in union for type $decodingTypeName")

  private[vulcan] final def decodeMissingUnionAlternative(
    alternativeName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError(s"Missing alternative $alternativeName in union for type $decodingTypeName")

  private[vulcan] final def decodeNameMismatch(
    fullName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Unable to decode $encodingTypeName using schema with name $fullName since names do not match"
    }

  private[vulcan] final def decodeSymbolNotInSchema(
    symbol: String,
    symbols: Seq[String],
    decodingTypeName: String
  ): AvroError =
    AvroError(
      s"$symbol is not part of schema symbols ${symbols.mkString("[", ", ", "]")} for type $decodingTypeName"
    )

  private[vulcan] final def decodeUnexpectedLogicalType(
    actualLogicalType: LogicalType,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      if (actualLogicalType == null)
        s"Got unexpected missing logical type while decoding $decodingTypeName"
      else
        s"Got unexpected logical type ${actualLogicalType.getName()} while decoding $decodingTypeName"
    }

  private[vulcan] final def decodeUnexpectedMapKey(key: Any): AvroError =
    AvroError {
      val typeName = if (key != null) key.getClass().getTypeName() else "null"
      s"Got unexpected map key with type $typeName while decoding Map, expected Utf8"
    }

  private[vulcan] final def decodeUnexpectedRecordName(
    recordFullName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      s"Got record writer schema with name $recordFullName, expected name $decodingTypeName"
    }

  private[vulcan] final def decodeUnexpectedSchemaType(
    decodingTypeName: String,
    actualSchemaType: Schema.Type,
    expectedSchemaType: Schema.Type
  ): AvroError =
    AvroError {
      s"Got unexpected schema type $actualSchemaType while decoding $decodingTypeName, expected schema type $expectedSchemaType"
    }

  private[vulcan] final def decodeUnexpectedType(
    value: Any,
    expectedType: String,
    decodingTypeName: String
  ): AvroError =
    AvroError.decodeUnexpectedTypes(
      value,
      NonEmptyList.one(expectedType),
      decodingTypeName
    )

  private[vulcan] final def decodeUnexpectedTypes(
    value: Any,
    expectedTypes: NonEmptyList[String],
    decodingTypeName: String
  ): AvroError =
    AvroError {
      val expected = expectedTypes.toList.mkString(", ")
      val types = if (expectedTypes.size > 1) "types" else "type"
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Got unexpected type $typeName while decoding $decodingTypeName, expected $types $expected"
    }

  private[vulcan] final def decodeExhaustedAlternatives(
    value: Any,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Exhausted alternatives for type $typeName while decoding $decodingTypeName"
    }

  private[vulcan] final def unexpectedChar(
    length: Int
  ): AvroError =
    AvroError(s"Got unexpected String with length $length while decoding Char, expected length 1")

  private[vulcan] final def unexpectedByte(value: Int): AvroError =
    AvroError {
      s"Got unexpected Byte value $value, expected value in range ${Byte.MinValue} to ${Byte.MaxValue}"
    }

  private[vulcan] final def unexpectedShort(value: Int): AvroError =
    AvroError {
      s"Got unexpected Short value $value, expected value in range ${Short.MinValue} to ${Short.MaxValue}"
    }

  private[vulcan] final def encodeDecimalPrecisionExceeded(
    actualPrecision: Int,
    expectedPrecision: Int
  ): AvroError =
    AvroError {
      s"Unable to encode decimal with precision $actualPrecision exceeding schema precision $expectedPrecision"
    }

  private[vulcan] final def encodeDecimalScalesMismatch(
    actualScale: Int,
    expectedScale: Int
  ): AvroError =
    AvroError(s"Unable to encode decimal with scale $actualScale as scale $expectedScale")

  private[vulcan] final def encodeExceedsFixedSize(
    length: Int,
    fixedSize: Int,
    encodingTypeName: String
  ): AvroError =
    AvroError(
      s"Got $length bytes while encoding $encodingTypeName, expected maximum fixed size $fixedSize"
    )

  private[vulcan] final def encodeMissingRecordField(
    fieldName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError(s"Record field '$fieldName' in schema is missing for type $encodingTypeName")

  private[vulcan] final def encodeExhaustedAlternatives(
    value: Any,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Exhausted alternatives for type $typeName while encoding $encodingTypeName"
    }

  private[vulcan] final def encodeSymbolNotInSchema(
    symbol: String,
    symbols: Seq[String],
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Symbol $symbol is not part of schema symbols [${symbols.mkString(", ")}] for type $encodingTypeName"
    }

  private[vulcan] final def fromThrowable(throwable: Throwable): AvroError =
    AvroError(throwable.toString)
}
