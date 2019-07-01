/*
 * Copyright 2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  * Use [[AvroError#apply]] to create an instance, and
  * [[AvroError#message]] to retrieve the message.
  */
sealed abstract class AvroError {
  def message: String

  def throwable: Throwable
}

final object AvroError {
  private[this] final class AvroErrorImpl(
    _message: () => String
  ) extends AvroError {
    override final def message: String =
      _message()

    override final def throwable: Throwable =
      AvroException(message)

    override final def toString: String =
      s"AvroError($message)"
  }

  final def apply(message: => String): AvroError =
    new AvroErrorImpl(() => message)

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

  private[vulcan] final def decodeExceedsFixedSize(length: Int, fixedSize: Int): AvroError =
    AvroError(s"Byte array with length $length exceeds fixed type with size $fixedSize")

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

  private[vulcan] final def decodeMissingUnionSubtype(
    subtypeName: String,
    decodingTypeName: String
  ): AvroError =
    AvroError(s"Missing subtype $subtypeName in union for type $decodingTypeName")

  private[vulcan] final def decodeNameMismatch(
    fullName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Unable to decode $encodingTypeName using schema with name $fullName since names do not match"
    }

  private[vulcan] final def decodeNotEnoughUnionSchemas(decodingTypeName: String): AvroError =
    AvroError(s"Not enough types in union schema while decoding type $decodingTypeName")

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

  private[vulcan] final def decodeUnexpectedOptionSchema(schema: Schema): AvroError =
    AvroError(s"Unexpected union schema $schema for Option")

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
    expectedSchemaTypes: NonEmptyList[Schema.Type]
  ): AvroError =
    AvroError {
      val schemaTypes = expectedSchemaTypes.toList.mkString(" or ")
      val types = if (expectedSchemaTypes.size > 1) "types" else "type"
      s"Got unexpected schema type $actualSchemaType while decoding $decodingTypeName, expected schema $types $schemaTypes"
    }

  private[vulcan] final def decodeUnexpectedType(
    value: Any,
    expectedType: String,
    decodingTypeName: String
  ): AvroError =
    AvroError {
      val typeName = if (value != null) value.getClass().getTypeName() else "null"
      s"Got unexpected type $typeName while decoding $decodingTypeName, expected type $expectedType"
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

  private[vulcan] final def encodeExceedsFixedSize(length: Int, fixedSize: Int): AvroError =
    AvroError(s"Byte array with length $length exceeds fixed type with size $fixedSize")

  private[vulcan] final def encodeMissingRecordField(
    fieldName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError(s"Record field '$fieldName' in schema is missing for type $encodingTypeName")

  private[vulcan] final def encodeMissingUnionSchema(
    subtypeName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Missing schema $subtypeName in union for type $encodingTypeName"
    }

  private[vulcan] final def encodeNameMismatch(
    fullName: String,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Unable to encode $encodingTypeName using schema with name $fullName since names do not match"
    }

  private[vulcan] final def encodeNotEnoughUnionSchemas(encodingTypeName: String): AvroError =
    AvroError(s"Not enough types in union schema while encoding type $encodingTypeName")

  private[vulcan] final def encodeSymbolNotInSchema(
    symbol: String,
    symbols: Seq[String],
    encodingTypeName: String
  ): AvroError =
    AvroError {
      s"Symbol $symbol is not part of schema symbols [${symbols.mkString(", ")}] for type $encodingTypeName"
    }

  private[vulcan] final def encodeUnexpectedLogicalType(
    actualLogicalType: LogicalType,
    encodingTypeName: String
  ): AvroError =
    AvroError {
      if (actualLogicalType == null)
        s"Got unexpected missing logical type while encoding $encodingTypeName"
      else
        s"Got unexpected logical type ${actualLogicalType.getName()} while encoding $encodingTypeName"
    }

  private[vulcan] final def encodeUnexpectedOptionSchema(schema: Schema): AvroError =
    AvroError(s"Unexpected union schema $schema for Option")

  private[vulcan] final def encodeUnexpectedSchemaType(
    encodingTypeName: String,
    actualSchemaType: Schema.Type,
    expectedSchemaTypes: NonEmptyList[Schema.Type]
  ): AvroError =
    AvroError {
      val schemaTypes = expectedSchemaTypes.toList.mkString(" or ")
      val types = if (expectedSchemaTypes.size > 1) "types" else "type"
      s"Got unexpected schema type $actualSchemaType while encoding $encodingTypeName, expected schema $types $schemaTypes"
    }

  private[vulcan] final def fromThrowable(throwable: Throwable): AvroError =
    AvroError(throwable.toString)
}
