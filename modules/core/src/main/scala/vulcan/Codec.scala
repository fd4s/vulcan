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

import cats.{~>, Invariant, Show}
import cats.data.{Chain, NonEmptyChain, NonEmptyList, NonEmptySet, NonEmptyVector}
import cats.free.FreeApplicative
import cats.implicits._
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate}
import java.util.UUID
import magnolia._
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic._
import org.apache.avro.util.Utf8
import scala.collection.immutable.SortedSet
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.WeakTypeTag
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}

/**
  * Describes encoding from a type `A` to Java Avro using [[encode]],
  * decoding from a Java Avro type to `A` using [[decode]], and the
  * Avro schema used in the process as [[schema]].
  */
sealed abstract class Codec[A] {

  /** The schema or an error if the schema is unavailable. */
  def schema: Either[AvroError, Schema]

  /** Attempts to encode the specified value using the provided schema. */
  def encode(a: A, schema: Schema): Either[AvroError, Any]

  /** Attempts to decode the specified value using the provided schema. */
  def decode(value: Any, schema: Schema): Either[AvroError, A]

  /**
    * Creates a new [[Codec]] which encodes values of type
    * `B` by first converting them to `A`. Similarly, the
    * decoding works by first decoding to `A` and then
    * converting to `B`.
    */
  final def imap[B](f: A => B)(g: B => A): Codec[B] =
    Codec.instance(
      schema,
      (b, schema) => encode(g(b), schema),
      (a, schema) => decode(a, schema).map(f)
    )

  /**
    * Like [[imap]], but where the mapping to `B` might
    * be unsuccessful.
    */
  final def imapError[B](f: A => Either[AvroError, B])(g: B => A): Codec[B] =
    Codec.instance(
      schema,
      (b, schema) => encode(g(b), schema),
      (a, schema) => decode(a, schema).flatMap(f)
    )
}

final object Codec {
  final def apply[A](implicit codec: Codec[A]): Codec[A] =
    codec

  implicit final val boolean: Codec[Boolean] =
    Codec.instance(
      Right(SchemaBuilder.builder().booleanType()),
      (boolean, schema) => {
        schema.getType() match {
          case Schema.Type.BOOLEAN =>
            Right(java.lang.Boolean.valueOf(boolean))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Boolean",
                  schemaType,
                  NonEmptyList.of(Schema.Type.BOOLEAN)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.BOOLEAN =>
            value match {
              case boolean: java.lang.Boolean =>
                Right(boolean)
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Boolean", "Boolean"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Boolean",
                  schemaType,
                  NonEmptyList.of(Schema.Type.BOOLEAN)
                )
            }
        }
      }
    )

  implicit final val bytes: Codec[Array[Byte]] =
    Codec.instance(
      Right(SchemaBuilder.builder().bytesType()),
      (bytes, schema) => {
        schema.getType() match {
          case Schema.Type.BYTES =>
            Right(ByteBuffer.wrap(bytes))

          case Schema.Type.FIXED =>
            val length = bytes.length
            val fixedSize = schema.getFixedSize()
            if (length <= fixedSize) {
              Right {
                val buffer = ByteBuffer.allocate(fixedSize).put(bytes)
                GenericData.get().createFixed(null, buffer.array(), schema)
              }
            } else {
              Left(AvroError.encodeExceedsFixedSize(length, fixedSize))
            }

          case schemaType =>
            val expectedTypes = NonEmptyList.of(Schema.Type.BYTES, Schema.Type.FIXED)
            Left(AvroError.encodeUnexpectedSchemaType("Array[Byte]", schemaType, expectedTypes))
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.BYTES =>
            value match {
              case buffer: ByteBuffer =>
                Right(buffer.array())
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "ByteBuffer", "Array[Byte]"))
            }

          case Schema.Type.FIXED =>
            value match {
              case fixed: GenericFixed =>
                val bytes = fixed.bytes()
                if (bytes.length <= schema.getFixedSize()) Right(bytes)
                else Left(AvroError.decodeExceedsFixedSize(bytes.length, schema.getFixedSize()))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "GenericFixed", "Array[Byte]"))
            }

          case schemaType =>
            val expectedTypes = NonEmptyList.of(Schema.Type.BYTES, Schema.Type.FIXED)
            Left(AvroError.decodeUnexpectedSchemaType("Array[Byte]", schemaType, expectedTypes))
        }
      }
    )

  implicit final def chain[A](implicit codec: Codec[A]): Codec[Chain[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (chain, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            chain.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Chain",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList
                  .traverse(codec.decode(_, elementType))
                  .map(Chain.fromSeq)

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "Chain"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Chain",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final val cnil: Codec[CNil] =
    Codec.instance(
      Right(Schema.createUnion()),
      (_, _) => Left(AvroError("CNil")),
      (_, _) => Left(AvroError("Unable to decode to any type in Coproduct"))
    )

  final def combine[A](caseClass: CaseClass[Codec, A]): Codec[A] = {
    val namespace =
      caseClass.annotations
        .collectFirst { case AvroNamespace(namespace) => namespace }
        .getOrElse(caseClass.typeName.owner)

    val typeName =
      s"$namespace.${caseClass.typeName.short}"

    Codec.instance(
      AvroError.catchNonFatal {
        if (caseClass.isValueClass) {
          caseClass.parameters.head.typeclass.schema
        } else {
          val fields =
            caseClass.parameters.toList.traverse { param =>
              param.typeclass.schema.map { schema =>
                new Schema.Field(
                  param.label,
                  schema,
                  param.annotations.collectFirst {
                    case AvroDoc(doc) => doc
                  }.orNull
                )
              }
            }

          fields.map { fields =>
            Schema.createRecord(
              caseClass.typeName.short,
              caseClass.annotations.collectFirst {
                case AvroDoc(doc) => doc
              }.orNull,
              namespace,
              false,
              fields.asJava
            )
          }
        }
      },
      if (caseClass.isValueClass) { (a, schema) =>
        {
          val param = caseClass.parameters.head
          param.typeclass.encode(param.dereference(a), schema)
        }
      } else { (a, schema) =>
        {
          schema.getType() match {
            case Schema.Type.RECORD =>
              if (schema.getFullName() == typeName) {
                val schemaFields =
                  schema.getFields().asScala

                val fields =
                  caseClass.parameters.toList.traverse { param =>
                    schemaFields
                      .collectFirst {
                        case field if field.name == param.label =>
                          param.typeclass
                            .encode(param.dereference(a), field.schema)
                            .tupleRight(field.pos())
                      }
                      .getOrElse(Left(AvroError.encodeMissingRecordField(param.label, typeName)))
                  }

                fields.map { values =>
                  val record = new GenericData.Record(schema)
                  values.foreach {
                    case (value, pos) =>
                      record.put(pos, value)
                  }

                  record
                }
              } else Left(AvroError.encodeNameMismatch(schema.getFullName(), typeName))

            case schemaType =>
              Left {
                AvroError
                  .encodeUnexpectedSchemaType(
                    typeName,
                    schemaType,
                    NonEmptyList.of(Schema.Type.RECORD)
                  )
              }
          }
        }
      },
      if (caseClass.isValueClass) { (value, schema) =>
        {
          caseClass.parameters.head.typeclass
            .decode(value, schema)
            .map(decoded => caseClass.rawConstruct(List(decoded)))
        }
      } else { (value, schema) =>
        {
          schema.getType() match {
            case Schema.Type.RECORD =>
              value match {
                case record: IndexedRecord =>
                  val recordSchema = record.getSchema()
                  if (recordSchema.getFullName() == typeName) {
                    val recordFields = recordSchema.getFields()

                    val fields =
                      caseClass.parameters.toList.traverse { param =>
                        val field = recordSchema.getField(param.label)
                        if (field != null) {
                          val value = record.get(recordFields.indexOf(field))
                          param.typeclass.decode(value, field.schema())
                        } else Left(AvroError.decodeMissingRecordField(param.label, typeName))
                      }

                    fields.map(caseClass.rawConstruct)
                  } else
                    Left(AvroError.decodeUnexpectedRecordName(recordSchema.getFullName(), typeName))

                case other =>
                  Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
              }

            case schemaType =>
              Left {
                AvroError
                  .decodeUnexpectedSchemaType(
                    typeName,
                    schemaType,
                    NonEmptyList.of(Schema.Type.RECORD)
                  )
              }
          }
        }
      }
    )
  }

  implicit final def coproduct[H, T <: Coproduct](
    implicit headCodec: Codec[H],
    tailCodec: Lazy[Codec[T]]
  ): Codec[H :+: T] =
    Codec.instance(
      AvroError.catchNonFatal {
        headCodec.schema.flatMap { first =>
          tailCodec.value.schema.flatMap { rest =>
            rest.getType() match {
              case Schema.Type.UNION =>
                val schemas = first :: rest.getTypes().asScala.toList
                Right(Schema.createUnion(schemas.asJava))

              case schemaType =>
                Left(AvroError(s"Unexpected schema type $schemaType in Coproduct"))
            }
          }
        }
      },
      (coproduct, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            schema.getTypes().asScala.toList match {
              case headSchema :: tailSchemas =>
                coproduct.eliminate(
                  headCodec.encode(_, headSchema),
                  tailCodec.value.encode(_, Schema.createUnion(tailSchemas.asJava))
                )

              case Nil =>
                Left(AvroError.encodeNotEnoughUnionSchemas("Coproduct"))
            }

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Coproduct",
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            value match {
              case container: GenericContainer =>
                headCodec.schema.flatMap {
                  headSchema =>
                    val name = container.getSchema().getFullName()
                    if (headSchema.getFullName() == name) {
                      val subschema =
                        schema
                          .getTypes()
                          .asScala
                          .find(_.getFullName() == name)
                          .toRight(AvroError.decodeMissingUnionSchema(name, "Coproduct"))

                      subschema
                        .flatMap(headCodec.decode(value, _))
                        .map(Inl(_))
                    } else {
                      schema.getTypes().asScala.toList match {
                        case _ :: tail =>
                          tailCodec.value
                            .decode(value, Schema.createUnion(tail.asJava))
                            .map(Inr(_))

                        case Nil =>
                          Left(AvroError.decodeNotEnoughUnionSchemas("Coproduct"))
                      }
                    }
                }

              case other =>
                schema.getTypes().asScala.toList match {
                  case head :: tail =>
                    headCodec.decode(other, head) match {
                      case Right(decoded) =>
                        Right(Inl(decoded))

                      case Left(_) =>
                        tailCodec.value
                          .decode(other, Schema.createUnion(tail.asJava))
                          .map(Inr(_))
                    }

                  case Nil =>
                    Left(AvroError.decodeNotEnoughUnionSchemas("Coproduct"))
                }
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Coproduct",
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      }
    )

  /**
    * Creates a new decimal [[Codec]] for type `BigDecimal`.
    */
  final def decimal(
    precision: Int,
    scale: Int
  ): Codec[BigDecimal] = {
    val conversion = new Conversions.DecimalConversion()
    Codec.instance(
      AvroError.catchNonFatal {
        Right {
          LogicalTypes
            .decimal(precision, scale)
            .addToSchema(SchemaBuilder.builder().bytesType())
        }
      },
      (bigDecimal, schema) => {
        schema.getType() match {
          case Schema.Type.BYTES =>
            schema.getLogicalType() match {
              case decimal: LogicalTypes.Decimal =>
                if (bigDecimal.scale == decimal.getScale()) {
                  if (bigDecimal.precision <= decimal.getPrecision()) {
                    Right(conversion.toBytes(bigDecimal.underlying(), schema, decimal))
                  } else {
                    Left {
                      AvroError
                        .encodeDecimalPrecisionExceeded(
                          bigDecimal.precision,
                          decimal.getPrecision()
                        )
                    }
                  }
                } else
                  Left(AvroError.encodeDecimalScalesMismatch(bigDecimal.scale, decimal.getScale()))

              case logicalType =>
                Left(AvroError.encodeUnexpectedLogicalType(logicalType, "BigDecimal"))
            }

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "BigDecimal",
                  schemaType,
                  NonEmptyList.of(Schema.Type.BYTES)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.BYTES =>
            schema.getLogicalType() match {
              case decimal: LogicalTypes.Decimal =>
                value match {
                  case buffer: ByteBuffer =>
                    val bigDecimal = BigDecimal(conversion.fromBytes(buffer, schema, decimal))
                    if (bigDecimal.precision <= decimal.getPrecision()) {
                      Right(bigDecimal)
                    } else {
                      Left {
                        AvroError
                          .decodeDecimalPrecisionExceeded(
                            bigDecimal.precision,
                            decimal.getPrecision()
                          )
                      }
                    }

                  case other =>
                    Left(AvroError.decodeUnexpectedType(other, "ByteBuffer", "BigDecimal"))
                }

              case logicalType =>
                Left(AvroError.decodeUnexpectedLogicalType(logicalType, "BigDecimal"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "BigDecimal",
                  schemaType,
                  NonEmptyList.of(Schema.Type.BYTES)
                )
            }
        }
      }
    )
  }

  /**
    * Derives a [[Codec]] instance for the specified type,
    * as long as the type is either a `case class` or a
    * `sealed trait`.
    */
  final def derive[A]: Codec[A] =
    macro Magnolia.gen[A]

  /**
    * Creates a new enum [[Codec]] for type `A`, deriving details
    * such as the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using type tags.
    */
  final def deriveEnum[A](
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec[A] = {
    val name =
      tag.tpe.typeSymbol.name.decodedName.toString

    val namespace =
      tag.tpe.typeSymbol.annotations
        .collectFirst {
          case annotation if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroNamespace" =>
            val namespace = annotation.tree.children.last.toString
            namespace.substring(1, namespace.length - 1)
        }
        .getOrElse {
          tag.tpe.typeSymbol.fullName.dropRight(name.length + 1)
        }

    val doc =
      tag.tpe.typeSymbol.annotations.collectFirst {
        case annotation if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroDoc" =>
          val doc = annotation.tree.children.last.toString
          doc.substring(1, doc.length - 1)
      }

    Codec.enum(
      name = name,
      symbols = symbols,
      encode = encode,
      decode = decode,
      namespace = Some(namespace),
      aliases = Nil,
      doc = doc,
      default = None
    )
  }

  final def dispatch[A](sealedTrait: SealedTrait[Codec, A]): Codec[A] = {
    val typeName = sealedTrait.typeName.full
    Codec.instance(
      AvroError.catchNonFatal {
        sealedTrait.subtypes.toList
          .traverse(_.typeclass.schema)
          .map(schemas => Schema.createUnion(schemas.asJava))
      },
      (a, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            sealedTrait.dispatch(a) {
              subtype =>
                subtype.typeclass.schema.flatMap {
                  subtypeSchema =>
                    val subtypeName =
                      subtypeSchema.getFullName()

                    val subschema =
                      schema.getTypes.asScala
                        .collectFirst {
                          case schema if schema.getFullName() == subtypeName =>
                            Right(schema)
                        }
                        .getOrElse(Left(AvroError.encodeMissingUnionSchema(subtypeName, typeName)))

                    subschema.flatMap(subtype.typeclass.encode(subtype.cast(a), _))
                }
            }

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            value match {
              case container: GenericContainer =>
                val name = container.getSchema().getFullName()

                val subschema =
                  schema
                    .getTypes()
                    .asScala
                    .find(_.getFullName() == name)
                    .toRight(AvroError.decodeMissingUnionSchema(name, typeName))

                def subtype =
                  sealedTrait.subtypes
                    .find(_.typeclass.schema.exists(_.getFullName() == name))
                    .toRight(AvroError.decodeMissingUnionSubtype(name, typeName))

                subschema.flatMap { subschema =>
                  subtype.flatMap { subtype =>
                    subtype.typeclass.decode(container, subschema)
                  }
                }

              case other =>
                val schemaTypes = schema.getTypes().asScala.toList
                val subtypes = sealedTrait.subtypes.toList

                subtypes
                  .zip(schemaTypes)
                  .collectFirstSome {
                    case (subtype, schema) =>
                      val decoded = subtype.typeclass.decode(other, schema)
                      if (decoded.isRight) Some(decoded) else None
                  }
                  .getOrElse {
                    Left(AvroError.decodeUnexpectedType(other, "GenericContainer", typeName))
                  }
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      }
    )
  }

  implicit final val double: Codec[Double] =
    Codec.instance(
      Right(SchemaBuilder.builder().doubleType()),
      (double, schema) => {
        schema.getType() match {
          case Schema.Type.DOUBLE =>
            Right(java.lang.Double.valueOf(double))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Double",
                  schemaType,
                  NonEmptyList.of(Schema.Type.DOUBLE)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.DOUBLE =>
            value match {
              case double: java.lang.Double =>
                Right(double)
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Double", "Double"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Double",
                  schemaType,
                  NonEmptyList.of(Schema.Type.DOUBLE)
                )
            }
        }
      }
    )

  /**
    * Creates a new enum [[Codec]] for type `A`.
    */
  final def enum[A](
    name: String,
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A],
    namespace: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    doc: Option[String] = None,
    default: Option[A] = None
  ): Codec[A] = {
    val typeName = namespace.fold(name)(namespace => s"$namespace.$name")
    Codec.instance(
      AvroError.catchNonFatal {
        val schema =
          Schema.createEnum(
            name,
            doc.orNull,
            namespace.orNull,
            symbols.asJava,
            default.map(encode).orNull
          )

        aliases.foreach(schema.addAlias)

        Right(schema)
      },
      (a, schema) => {
        schema.getType() match {
          case Schema.Type.ENUM =>
            if (schema.getFullName() == typeName) {
              val symbols = schema.getEnumSymbols().asScala
              val symbol = encode(a)

              if (symbols.contains(symbol))
                Right(GenericData.get().createEnum(symbol, schema))
              else
                Left(AvroError.encodeSymbolNotInSchema(symbol, symbols, typeName))
            } else Left(AvroError.encodeNameMismatch(schema.getFullName(), typeName))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.ENUM)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ENUM =>
            value match {
              case enum: GenericEnumSymbol[_] =>
                val fullName = schema.getFullName()
                if (fullName == typeName) {
                  val symbols = schema.getEnumSymbols().asScala
                  val symbol = enum.toString()

                  if (symbols.contains(symbol))
                    decode(symbol)
                  else
                    Left(AvroError.decodeSymbolNotInSchema(symbol, symbols, typeName))
                } else Left(AvroError.decodeNameMismatch(schema.getFullName(), typeName))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "GenericEnumSymbol", typeName))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.ENUM)
                )
            }
        }
      }
    )
  }

  implicit final val float: Codec[Float] =
    Codec.instance(
      Right(SchemaBuilder.builder().floatType()),
      (float, schema) => {
        schema.getType() match {
          case Schema.Type.FLOAT =>
            Right(java.lang.Float.valueOf(float))
          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Float",
                  schemaType,
                  NonEmptyList.of(Schema.Type.FLOAT)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.FLOAT =>
            value match {
              case float: java.lang.Float =>
                Right(float)
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Float", "Float"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Float",
                  schemaType,
                  NonEmptyList.of(Schema.Type.FLOAT)
                )
            }
        }
      }
    )

  /**
    * Creates a new [[Codec]] instance using the specified
    * `Schema`, and encode and decode functions.
    */
  final def instance[A](
    schema: Either[AvroError, Schema],
    encode: (A, Schema) => Either[AvroError, Any],
    decode: (Any, Schema) => Either[AvroError, A]
  ): Codec[A] = {
    val (_schema, _encode, _decode) =
      (schema, encode, decode)

    new Codec[A] {
      override final val schema: Either[AvroError, Schema] =
        _schema

      override final def encode(a: A, schema: Schema): Either[AvroError, Any] =
        _encode(a, schema)

      override final def decode(value: Any, schema: Schema): Either[AvroError, A] =
        _decode(value, schema)

      override final def toString: String =
        schema match {
          case Right(schema) => s"Codec(${schema.toString(true)})"
          case Left(error)   => error.toString()
        }
    }
  }

  implicit final val instant: Codec[Instant] =
    Codec.instance(
      Right(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())),
      (instant, schema) => {
        schema.getType() match {
          case Schema.Type.LONG =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.timestampMillis())
              Right(java.lang.Long.valueOf(instant.toEpochMilli()))
            else Left(AvroError.encodeUnexpectedLogicalType(logicalType, "Instant"))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Instant",
                  schemaType,
                  NonEmptyList.of(Schema.Type.LONG)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.LONG =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.timestampMillis()) {
              value match {
                case long: java.lang.Long =>
                  Right(Instant.ofEpochMilli(long))
                case other =>
                  Left(AvroError.decodeUnexpectedType(other, "Long", "Instant"))
              }
            } else Left(AvroError.decodeUnexpectedLogicalType(logicalType, "Instant"))

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Instant",
                  schemaType,
                  NonEmptyList.of(Schema.Type.LONG)
                )
            }
        }
      }
    )

  implicit final val int: Codec[Int] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      (int, schema) => {
        schema.getType() match {
          case Schema.Type.INT =>
            Right(java.lang.Integer.valueOf(int))
          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Int",
                  schemaType,
                  NonEmptyList.of(Schema.Type.INT)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.INT =>
            value match {
              case integer: java.lang.Integer =>
                Right(integer)
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Int", "Int"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Int",
                  schemaType,
                  NonEmptyList.of(Schema.Type.INT)
                )
            }
        }
      }
    )

  implicit final def list[A](implicit codec: Codec[A]): Codec[List[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (list, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            list.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "List",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList.traverse(codec.decode(_, elementType))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "List"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "List",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )
  implicit final val localDate: Codec[LocalDate] =
    Codec.instance(
      Right(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())),
      (localDate, schema) => {
        schema.getType() match {
          case Schema.Type.INT =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.date()) {
              Right(java.lang.Integer.valueOf(localDate.toEpochDay.toInt))
            } else Left(AvroError.encodeUnexpectedLogicalType(logicalType, "LocalDate"))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "LocalDate",
                  schemaType,
                  NonEmptyList.of(Schema.Type.INT)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.INT =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.date()) {
              value match {
                case int: java.lang.Integer =>
                  Right(LocalDate.ofEpochDay(int.toLong))
                case other =>
                  Left(AvroError.decodeUnexpectedType(other, "Integer", "LocalDate"))
              }
            } else Left(AvroError.decodeUnexpectedLogicalType(logicalType, "LocalDate"))
          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "LocalDate",
                  schemaType,
                  NonEmptyList.of(Schema.Type.INT)
                )
            }
        }
      }
    )

  implicit final val long: Codec[Long] =
    Codec.instance(
      Right(SchemaBuilder.builder().longType()),
      (long, schema) => {
        schema.getType() match {
          case Schema.Type.LONG =>
            Right(java.lang.Long.valueOf(long))
          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Long",
                  schemaType,
                  NonEmptyList.of(Schema.Type.LONG)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.LONG =>
            value match {
              case long: java.lang.Long =>
                Right(long)
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Long", "Long"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Long",
                  schemaType,
                  NonEmptyList.of(Schema.Type.LONG)
                )
            }
        }
      }
    )

  implicit final def nonEmptyChain[A](implicit codec: Codec[A]): Codec[NonEmptyChain[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (chain, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            chain.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "NonEmptyChain",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList
                  .traverse(codec.decode(_, elementType))
                  .flatMap { list =>
                    if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyChain"))
                    else Right(NonEmptyChain.fromChainUnsafe(Chain.fromSeq(list)))
                  }

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyChain"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "NonEmptyChain",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final def nonEmptyList[A](implicit codec: Codec[A]): Codec[NonEmptyList[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (list, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            list.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "NonEmptyList",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList
                  .traverse(codec.decode(_, elementType))
                  .flatMap { list =>
                    if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyList"))
                    else Right(NonEmptyList.fromListUnsafe(list))
                  }

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyList"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "NonEmptyList",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final def nonEmptySet[A](
    implicit codec: Codec[A],
    ordering: Ordering[A]
  ): Codec[NonEmptySet[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (set, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            set.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "NonEmptySet",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList
                  .traverse(codec.decode(_, elementType))
                  .flatMap { list =>
                    if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptySet"))
                    else Right(NonEmptySet.fromSetUnsafe(SortedSet(list: _*)))
                  }

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptySet"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "NonEmptySet",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final def nonEmptyVector[A](implicit codec: Codec[A]): Codec[NonEmptyVector[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (vector, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            vector.toVector.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError.encodeUnexpectedSchemaType(
                "NonEmptyVector",
                schemaType,
                NonEmptyList.of(Schema.Type.ARRAY)
              )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toVector
                  .traverse(codec.decode(_, elementType))
                  .flatMap { vector =>
                    if (vector.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyVector"))
                    else Right(NonEmptyVector.fromVectorUnsafe(vector))
                  }

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyVector"))
            }

          case schemaType =>
            Left {
              AvroError.decodeUnexpectedSchemaType(
                "NonEmptyVector",
                schemaType,
                NonEmptyList.of(Schema.Type.ARRAY)
              )
            }
        }
      }
    )

  implicit final def option[A](implicit codec: Codec[A]): Codec[Option[A]] =
    Codec.instance(
      AvroError.catchNonFatal {
        codec.schema.map { schema =>
          val nullSchema = SchemaBuilder.builder().nullType()
          val schemas = List(nullSchema, schema).asJava
          Schema.createUnion(schemas)
        }
      },
      (option, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            val schemas =
              schema.getTypes()

            val nonNullSchema =
              if (schemas.size == 2) {
                if (schemas.get(0).getType() == Schema.Type.NULL)
                  Right(schemas.get(1))
                else Right(schemas.get(0))
              } else Left(AvroError.encodeUnexpectedOptionSchema(schema))

            nonNullSchema.flatMap { schema =>
              option match {
                case Some(a) => codec.encode(a, schema)
                case None    => Right(null)
              }
            }

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Option",
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            val schemas = schema.getTypes()

            val nonNullSchema =
              if (schemas.size == 2) {
                if (schemas.get(0).getType() == Schema.Type.NULL)
                  Right(schemas.get(1))
                else Right(schemas.get(0))
              } else Left(AvroError.decodeUnexpectedOptionSchema(schema))

            nonNullSchema.flatMap { schema =>
              value match {
                case null  => Right(None)
                case other => codec.decode(other, schema).map(Some(_))
              }
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Option",
                  schemaType,
                  NonEmptyList.of(Schema.Type.UNION)
                )
            }
        }
      }
    )

  /**
    * Creates a new record [[Codec]] for type `A`.
    */
  final def record[A](
    name: String,
    namespace: Option[String] = None,
    doc: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    props: Seq[(String, String)] = Seq.empty
  )(f: FieldBuilder[A] => FreeApplicative[Field[A, ?], A]): Codec[A] = {
    val typeName = namespace.fold(name)(namespace => s"$namespace.$name")
    val free = f(FieldBuilder.instance)
    Codec.instance(
      AvroError.catchNonFatal {
        val fields =
          free.analyze {
            λ[Field[A, ?] ~> λ[a => Either[AvroError, Chain[Schema.Field]]]] {
              field =>
                field.codec.schema.flatMap {
                  schema =>
                    field.default
                      .traverse(field.codec.encode(_, schema))
                      .map {
                        default =>
                          Chain.one {
                            val schemaField =
                              new Schema.Field(
                                field.name,
                                schema,
                                field.doc.orNull,
                                default.map {
                                  case null  => Schema.Field.NULL_DEFAULT_VALUE
                                  case other => other
                                }.orNull,
                                field.order.getOrElse(Schema.Field.Order.ASCENDING)
                              )

                            field.aliases.foreach(schemaField.addAlias)

                            field.props.foreach {
                              case (name, value) =>
                                schemaField.addProp(name, value)
                            }

                            schemaField
                          }
                      }
                }
            }
          }

        fields.map { fields =>
          val record =
            Schema.createRecord(
              name,
              doc.orNull,
              namespace.orNull,
              false,
              fields.toList.asJava
            )

          aliases.foreach(record.addAlias)

          props.foreach {
            case (name, value) =>
              record.addProp(name, value)
          }

          record
        }
      },
      (a, schema) => {
        schema.getType() match {
          case Schema.Type.RECORD =>
            if (schema.getFullName() == typeName) {
              val schemaFields =
                schema.getFields().asScala

              val fields =
                free.analyze {
                  λ[Field[A, ?] ~> λ[a => Either[AvroError, Chain[(Any, Int)]]]] { field =>
                    schemaFields
                      .collectFirst {
                        case schemaField if schemaField.name == field.name =>
                          field.codec
                            .encode(field.access(a), schemaField.schema)
                            .map(result => Chain.one((result, schemaField.pos())))
                      }
                      .getOrElse(Left(AvroError.encodeMissingRecordField(field.name, typeName)))
                  }
                }

              fields.map { values =>
                val record = new GenericData.Record(schema)
                values.foldLeft(()) {
                  case ((), (value, pos)) =>
                    record.put(pos, value)
                }

                record
              }
            } else Left(AvroError.encodeNameMismatch(schema.getFullName(), typeName))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.RECORD)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.RECORD =>
            value match {
              case record: IndexedRecord =>
                val recordSchema = record.getSchema()
                if (recordSchema.getFullName() == typeName) {
                  val recordFields = recordSchema.getFields()

                  free.foldMap {
                    λ[Field[A, ?] ~> Either[AvroError, ?]] { field =>
                      val schemaField = recordSchema.getField(field.name)
                      if (schemaField != null) {
                        val value = record.get(recordFields.indexOf(schemaField))
                        field.codec.decode(value, schemaField.schema())
                      } else {
                        field.default.toRight {
                          AvroError.decodeMissingRecordField(field.name, typeName)
                        }
                      }
                    }
                  }
                } else
                  Left(AvroError.decodeUnexpectedRecordName(recordSchema.getFullName(), typeName))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  typeName,
                  schemaType,
                  NonEmptyList.of(Schema.Type.RECORD)
                )
            }
        }
      }
    )
  }

  implicit final def seq[A](implicit codec: Codec[A]): Codec[Seq[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (seq, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            seq.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Seq",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList.traverse(codec.decode(_, elementType))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "Seq"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Seq",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final def set[A](implicit codec: Codec[A]): Codec[Set[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (set, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            set.toList.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Set",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toList.traverse(codec.decode(_, elementType)).map(_.toSet)

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "Set"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Set",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  implicit final val string: Codec[String] =
    Codec.instance(
      Right(SchemaBuilder.builder().stringType()),
      (string, schema) => {
        schema.getType() match {
          case Schema.Type.STRING =>
            Right(new Utf8(string))
          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "String",
                  schemaType,
                  NonEmptyList.of(Schema.Type.STRING)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.STRING =>
            value match {
              case utf8: Utf8 =>
                Right(utf8.toString())
              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Utf8", "String"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "String",
                  schemaType,
                  NonEmptyList.of(Schema.Type.STRING)
                )
            }
        }
      }
    )

  implicit final val unit: Codec[Unit] =
    Codec.instance(
      Right(SchemaBuilder.builder().nullType()),
      (_, schema) => {
        schema.getType() match {
          case Schema.Type.NULL =>
            Right(null)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Unit",
                  schemaType,
                  NonEmptyList.of(Schema.Type.NULL)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.NULL =>
            if (value == null) Right(())
            else Left(AvroError.decodeUnexpectedType(value, "null", "Unit"))

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Unit",
                  schemaType,
                  NonEmptyList.of(Schema.Type.NULL)
                )
            }
        }
      }
    )

  implicit final val uuid: Codec[UUID] =
    Codec.instance(
      Right(LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType())),
      (uuid, schema) => {
        schema.getType() match {
          case Schema.Type.STRING =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.uuid())
              Right(new Utf8(uuid.toString()))
            else Left(AvroError.encodeUnexpectedLogicalType(logicalType, "UUID"))

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "UUID",
                  schemaType,
                  NonEmptyList.of(Schema.Type.STRING)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.STRING =>
            val logicalType = schema.getLogicalType()
            if (logicalType == LogicalTypes.uuid()) {
              value match {
                case utf8: Utf8 =>
                  AvroError.catchNonFatal {
                    Right(UUID.fromString(utf8.toString()))
                  }
                case other =>
                  Left(AvroError.decodeUnexpectedType(other, "Utf8", "UUID"))
              }
            } else Left(AvroError.decodeUnexpectedLogicalType(logicalType, "UUID"))

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "UUID",
                  schemaType,
                  NonEmptyList.of(Schema.Type.STRING)
                )
            }
        }
      }
    )

  implicit final def vector[A](implicit codec: Codec[A]): Codec[Vector[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      (vector, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            vector.traverse(codec.encode(_, elementType)).map(_.asJava)

          case schemaType =>
            Left {
              AvroError
                .encodeUnexpectedSchemaType(
                  "Vector",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      },
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.ARRAY =>
            val elementType = schema.getElementType()
            value match {
              case collection: java.util.Collection[_] =>
                collection.asScala.toVector.traverse(codec.decode(_, elementType))

              case other =>
                Left(AvroError.decodeUnexpectedType(other, "Collection", "Vector"))
            }

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Vector",
                  schemaType,
                  NonEmptyList.of(Schema.Type.ARRAY)
                )
            }
        }
      }
    )

  private[vulcan] final type Typeclass[A] = Codec[A]

  implicit final val codecInvariant: Invariant[Codec] =
    new Invariant[Codec] {
      override final def imap[A, B](codec: Codec[A])(f: A => B)(g: B => A): Codec[B] =
        codec.imap(f)(g)
    }

  implicit final def codecShow[A]: Show[Codec[A]] =
    Show.fromToString

  sealed abstract class Field[A, B] {
    def name: String

    def access: A => B

    def codec: Codec[B]

    def doc: Option[String]

    def default: Option[B]

    def order: Option[Schema.Field.Order]

    def aliases: Seq[String]

    def props: Seq[(String, String)]
  }

  private[vulcan] final object Field {
    private[this] final class FieldImpl[A, B](
      override final val name: String,
      override final val access: A => B,
      override final val codec: Codec[B],
      override final val doc: Option[String],
      override final val default: Option[B],
      override final val order: Option[Schema.Field.Order],
      override final val aliases: Seq[String],
      override final val props: Seq[(String, String)]
    ) extends Field[A, B]

    final def apply[A, B](
      name: String,
      access: A => B,
      codec: Codec[B],
      doc: Option[String],
      default: Option[B],
      order: Option[Schema.Field.Order],
      aliases: Seq[String],
      props: Seq[(String, String)]
    ): Field[A, B] =
      new FieldImpl(
        name = name,
        access = access,
        codec = codec,
        doc = doc,
        default = default,
        order = order,
        aliases = aliases,
        props = props
      )
  }

  sealed abstract class FieldBuilder[A] {
    def apply[B](
      name: String,
      access: A => B,
      doc: Option[String] = None,
      default: Option[B] = None,
      order: Option[Schema.Field.Order] = None,
      aliases: Seq[String] = Seq.empty,
      props: Seq[(String, String)] = Seq.empty
    )(implicit codec: Codec[B]): FreeApplicative[Field[A, ?], B]
  }

  private[vulcan] final object FieldBuilder {
    private[this] final class FieldBuilderImpl[A] extends FieldBuilder[A] {
      override final def apply[B](
        name: String,
        access: A => B,
        doc: Option[String],
        default: Option[B],
        order: Option[Schema.Field.Order],
        aliases: Seq[String],
        props: Seq[(String, String)]
      )(implicit codec: Codec[B]): FreeApplicative[Field[A, ?], B] =
        FreeApplicative.lift {
          Field(
            name = name,
            access = access,
            codec = codec,
            doc = doc,
            default = default,
            order = order,
            aliases = aliases,
            props = props
          )
        }

      override final def toString: String =
        "FieldBuilder"
    }

    private[this] final val Instance: FieldBuilder[Any] =
      new FieldBuilderImpl[Any]

    final def instance[A]: FieldBuilder[A] =
      Instance.asInstanceOf[FieldBuilder[A]]
  }
}
