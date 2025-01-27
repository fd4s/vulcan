/*
 * Copyright 2019-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.{Eq, Show}
import cats.data.NonEmptyList
import cats.instances.string._
import org.apache.avro.{Schema, LogicalType}
import scala.util.control.NonFatal
import java.time.LocalDate

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

  override final def toString: String =
    s"AvroError($message)"
}

@deprecated("Do not use - kept for binary compatibility", "1.5.0")
sealed abstract class AvroDecodingError extends AvroError {
  def withDecodingTypeName(decodingTypeName: String): AvroDecodingError
}

object AvroDecodingError {
  @deprecated("Do not use - kept for binary compatibility", "1.5.0")
  final def apply(decodingTypeName: String, message: String => String): AvroDecodingError = {
    val _message = message

    new AvroDecodingError {
      override final val message: String =
        _message(decodingTypeName)

      override final def throwable: Throwable =
        AvroException(message)

      override final def withDecodingTypeName(_decodingTypeName: String) =
        apply(_decodingTypeName, _message)
    }
  }
}

object AvroError {
  final def apply(message: => String): AvroError = {
    val _message = () => message

    new AvroError {
      override final def message: String =
        _message()

      override final def throwable: Throwable =
        AvroException(message)

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

  private[vulcan] def errorDecodingTo(decodingTypeName: String, cause: AvroError): AvroError =
    ErrorDecodingType(decodingTypeName, cause)

  private[vulcan] final case class ErrorDecodingType(decodingTypeName: String, cause: AvroError)
      extends AvroError {

    def message = s"Error decoding $decodingTypeName: ${cause.message}"
    def throwable: Throwable =
      AvroException(message)

  }

  private[vulcan] final def decodeDecimalPrecisionExceeded(
    actualPrecision: Int,
    expectedPrecision: Int
  ): AvroError =
    AvroError {
      s"Unable to decode decimal with precision $actualPrecision exceeding schema precision $expectedPrecision"
    }

  private[vulcan] val decodeEmptyCollection: AvroError =
    AvroError(s"Got unexpected empty collection")

  private[vulcan] final def decodeNotEqualFixedSize(
    length: Int,
    fixedSize: Int
  ): AvroError =
    AvroError(
      s"Got $length bytes, expected fixed size $fixedSize"
    )

  private[vulcan] final def decodeMissingRecordField(
    fieldName: String
  ): AvroError =
    AvroError(
      s"Record writer schema is missing field '$fieldName'"
    )

  private[vulcan] final def decodeMissingUnionSchema(
    subtypeName: String
  ): AvroError =
    AvroError(s"Missing schema $subtypeName in union")

  private[vulcan] final def decodeMissingUnionAlternative(
    alternativeName: String
  ): AvroError =
    AvroError(s"Missing alternative $alternativeName in union")

  private[vulcan] final def decodeNameMismatch(
    fullName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      s"Unable to decode $decodingTypeName using schema with name $fullName since names do not match"
    }

  private[vulcan] final def decodeSymbolNotInSchema(
    symbol: String,
    symbols: Seq[String]
  ): AvroError =
    AvroError(
      s"$symbol is not one of schema symbols ${symbols.mkString("[", ", ", "]")}"
    )

  private[vulcan] final def decodeUnexpectedLogicalType(
    actualLogicalType: LogicalType
  ): AvroError =
    AvroError(
      if (actualLogicalType == null)
        s"Got unexpected missing logical type"
      else
        s"Got unexpected logical type ${actualLogicalType.getName()}"
    )

  private[vulcan] final def decodeUnexpectedMapKey(key: Any): AvroError =
    AvroError {
      val typeName = if (key != null) key.getClass().getTypeName() else "null"
      s"Got unexpected map key with type $typeName, expected Utf8"
    }

  private[vulcan] final def decodeUnexpectedRecordName(
    expectedRecordName: String,
    recordName: String
  ): AvroError =
    AvroError(
      s"Got record writer schema with name $recordName, expected name $expectedRecordName"
    )

  private[vulcan] final def decodeUnexpectedSchemaType(
    actualSchemaType: Schema.Type,
    expectedSchemaType: Schema.Type
  ): AvroError =
    AvroError(
      s"Got unexpected schema type $actualSchemaType, expected schema type $expectedSchemaType"
    )

  private[vulcan] final def decodeUnexpectedType(
    value: Any,
    expectedType: String
  ): AvroError =
    AvroError.decodeUnexpectedTypes(
      value,
      NonEmptyList.one(expectedType)
    )

  private[vulcan] final def decodeUnexpectedTypes(
    value: Any,
    expectedTypes: NonEmptyList[String]
  ): AvroError =
    AvroError {
      val expected = expectedTypes.toList.mkString(", ")
      val types = if (expectedTypes.size > 1) "types" else "type"
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Got unexpected type $typeName, expected $types $expected"
    }

  private[vulcan] final def decodeExhaustedAlternatives(
    value: Any
  ): AvroError = {
    val typeName = if (value != null) value.getClass().getTypeName() else "null"
    AvroError(s"Exhausted alternatives for type $typeName")
  }

  private[vulcan] final def unexpectedChar(
    length: Int
  ): AvroError =
    AvroError(s"Got unexpected String with length $length, expected length 1")

  private[vulcan] final def unexpectedByte(value: Int): AvroError =
    AvroError {
      s"Got unexpected Int value $value, expected value in range ${Byte.MinValue} to ${Byte.MaxValue}"
    }

  private[vulcan] final def unexpectedShort(value: Int): AvroError =
    AvroError {
      s"Got unexpected Int value $value, expected value in range ${Short.MinValue} to ${Short.MaxValue}"
    }

  private[vulcan] def errorEncodingFrom(encodingTypeName: String, cause: AvroError): AvroError =
    ErrorEncodingType(encodingTypeName, cause)

  private[vulcan] final case class ErrorEncodingType(encodingTypeName: String, cause: AvroError)
      extends AvroError {

    def message = s"Error encoding $encodingTypeName: ${cause.message}"
    def throwable: Throwable =
      AvroException(message)

  }

  private[vulcan] final def encodeDateSizeExceeded(
    date: LocalDate
  ): AvroError =
    AvroError(
      s"Unable to encode date as epoch days of ${date.toEpochDay} exceeds the maximum integer size"
    )

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
    fixedSize: Int
  ): AvroError =
    AvroError(
      s"Got $length bytes, expected maximum fixed size $fixedSize"
    )

  private[vulcan] final def encodeExhaustedAlternatives(
    value: Any
  ): AvroError =
    AvroError {
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Exhausted alternatives for type $typeName"
    }

  private[vulcan] final def encodeSymbolNotInSchema(
    symbol: String,
    symbols: Seq[String]
  ): AvroError =
    AvroError {
      s"Symbol $symbol is not part of schema symbols [${symbols.mkString(", ")}]"
    }

  private[vulcan] final def fromThrowable(cause: Throwable): AvroError = {
    new AvroError {
      def message: String = cause.toString

      def throwable: Throwable = AvroException(message, Some(cause))
    }
  }
}
