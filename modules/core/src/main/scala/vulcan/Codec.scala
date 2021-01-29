/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.{~>, Invariant, Show}
import cats.data.{Chain, NonEmptyChain, NonEmptyList, NonEmptySet, NonEmptyVector}
import cats.free.FreeApplicative
import cats.implicits._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate}
import java.util.UUID

import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedSet
import vulcan.internal.converters.collection._
import vulcan.internal.schema.adaptForSchema

import scala.util.Try

/**
  * Provides a schema, along with encoding and decoding functions
  * for a given type.
  */
@implicitNotFound(
  "could not find implicit Codec[${A}]; ensure no imports are missing or manually define an instance"
)
sealed abstract class Codec[A] {
  type Repr <: Avro

  /** The schema or an error if the schema could not be generated. */
  def schema: Either[AvroError, Schema]

  /** Attempts to encode the specified value using the provided schema. */
  def encode(a: A): Either[AvroError, Repr]

  /** Attempts to decode the specified value using the provided schema. */
  def decode(value: Avro): Either[AvroError, A]

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    */
  final def imap[B](f: A => B)(g: B => A): Codec.Aux[B, Repr] =
    Codec.instance(
      schema,
      b => encode(g(b)),
      a => decode(a).map(f)
    )

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    *
    * Similar to [[Codec#imap]], except the mapping from
    * `A` to `B` might be unsuccessful.
    */
  final def imapError[B](f: A => Either[AvroError, B])(g: B => A): Codec.Aux[B, Repr] =
    Codec.instance(
      schema,
      b => encode(g(b)),
      a => decode(a).flatMap(f)
    )

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    *
    * Similar to [[Codec#imap]], except the mapping from
    * `A` to `B` might be unsuccessful.
    */
  final def imapTry[B](f: A => Try[B])(g: B => A): Codec.Aux[B, Repr] =
    imapError(f(_).toEither.leftMap(AvroError.fromThrowable))(g)

  private[vulcan] def withDecodingTypeName(decodingTypeName: String): Codec.Aux[A, Repr] =
    Codec.instance(
      schema,
      encode,
      decode(_) leftMap {
        case d: AvroDecodingError => d.withDecodingTypeName(decodingTypeName)
        case other                => other
      }
    )

}

/**
  * @groupname General General Codecs
  * @groupprio General 0
  * @groupdesc General Default codecs for standard library types.
  *
  * @groupname Collection Collection Codecs
  * @groupprio Collection 1
  * @groupdesc Collection Default codecs for standard library collection types.
  *
  * @groupname Cats Cats Codecs
  * @groupprio Cats 2
  * @groupdesc Cats Default codecs for Cats data types and type class instances for [[Codec]].
  *
  * @groupname JavaTime Java Time Codecs
  * @groupprio JavaTime 3
  * @groupdesc JavaTime Default codecs for `java.time` types.
  *
  * @groupname JavaUtil Java Util Codecs
  * @groupprio JavaUtil 4
  * @groupdesc JavaUtil Default codecs for `java.util` types.
  *
  * @groupname Create Create Codecs
  * @groupprio Create 5
  * @groupdesc Create Functions for creating new codecs.
  *
  * @groupname Derive Derive Codecs
  * @groupprio Derive 6
  * @groupdesc Derive Functions for deriving new codecs.
  *
  * @groupname Utilities
  * @groupprio Utilities 7
  * @groupdesc Utilities Miscellaneous utility functions.
  */
object Codec extends CodecCompanionCompat {

  type Aux[A, T0 <: Avro] = Codec[A] {
    type Repr = T0
  }

  /**
    * Returns the [[Codec]] for the specified type.
    *
    * @group Utilities
    */
  final def apply[A](implicit codec: Codec[A]): Codec[A] =
    codec

  /**
    * @group General
    */
  implicit final val boolean: Codec.Aux[Boolean, Avro.Boolean] =
    Codec.instance(
      Right(SchemaBuilder.builder().booleanType()),
      Avro.Boolean(_).asRight, {
        case Avro.Boolean(boolean) =>
          Right(boolean)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Boolean", "Boolean"))
      }
    )

  /**
    * @group General
    */
  implicit final val byte: Codec.Aux[Byte, Avro.Int] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      byte => Right(Avro.Int(byte.toInt, None)), {
        case Avro.Int(integer, _) =>
          if (Byte.MinValue.toInt <= integer && integer <= Byte.MaxValue.toInt)
            Right(integer.toByte)
          else Left(AvroError.unexpectedByte(integer))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Int", "Byte"))

      }
    )

  /**
    * @group General
    */
  implicit final val bytes: Codec.Aux[Array[Byte], Avro.Bytes] =
    Codec.instance(
      Right(SchemaBuilder.builder().bytesType()),
      bytes => Avro.Bytes(ByteBuffer.wrap(bytes), None).asRight, {
        case Avro.String(string, _) =>
          Right(string.getBytes(StandardCharsets.UTF_8))
        case Avro.Bytes(buffer, _) => Right(buffer.array())
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "ByteBuffer", "Array[Byte]"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def chain[A](
    implicit codec: Codec[A]
  ): Codec.Aux[Chain[A], Avro.Array[codec.Repr]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode(_)).map(items => Avro.Array(items.toVector)), {
        case Avro.Array(elements) =>
          elements.traverse(codec.decode).map(Chain.fromSeq)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "Chain"))
      }
    )

  /**
    * @group General
    */
  implicit final val char: Codec.Aux[Char, Avro.String] =
    Codec.instance(
      Right(SchemaBuilder.builder().stringType()),
      char => Right(Avro.String(char.toString, None)), {
        case Avro.String(string, _) =>
          if (string.length == 1) Right(string.charAt(0))
          else Left(AvroError.unexpectedChar(string.length))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Utf8", "Char"))
      }
    )

  /**
    * Returns a new decimal [[Codec]] for type `BigDecimal`.
    *
    * @group Create
    */
  final def decimal(
    precision: Int,
    scale: Int
  ): Codec.Aux[BigDecimal, Avro.Bytes] = {
    val conversion = new Conversions.DecimalConversion()
    val logicalType = LogicalTypes.decimal(precision, scale)
    val schema = AvroError.catchNonFatal {
      Right {
        logicalType.addToSchema(SchemaBuilder.builder().bytesType())
      }
    }
    Codec.instance(
      schema,
      bigDecimal =>
        if (bigDecimal.scale == scale) {
          if (bigDecimal.precision <= precision) {
            Right(
              Avro.Bytes(
                conversion.toBytes(bigDecimal.underlying(), null, logicalType),
                Some(logicalType)
              )
            )
          } else {
            Left {
              AvroError
                .encodeDecimalPrecisionExceeded(
                  bigDecimal.precision,
                  precision
                )
            }
          }
        } else
          Left(AvroError.encodeDecimalScalesMismatch(bigDecimal.scale, scale)), {
        case Avro.Bytes(buffer, Some(decimal: LogicalTypes.Decimal)) =>
          val bigDecimal = BigDecimal(conversion.fromBytes(buffer, null, decimal))
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
        case Avro.Bytes(_, logicalType) =>
          Left(AvroError.decodeUnexpectedLogicalType(logicalType.orNull, "BigDecimal"))
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "ByteBuffer", "BigDecimal"))
      }
    )
  }

  /**
    * Returns the result of decoding the specified value
    * to the specified type.
    *
    * @group Utilities
    */
  final def decode[A](value: Avro)(implicit codec: Codec[A]): Either[AvroError, A] =
    codec.decode(value)

  final def fromJava[A](value: Any)(implicit codec: Codec[A]): Either[AvroError, A] =
    codec.schema.flatMap(schema => Avro.fromJava(value, schema).flatMap(codec.decode(_)))

  /**
    * @group General
    */
  implicit final val double: Codec.Aux[Double, Avro.Double] =
    Codec.instance(
      Right(SchemaBuilder.builder().doubleType()),
      Avro.Double(_).asRight, {
        case Avro.Double(value)  => value.asRight
        case Avro.Float(value)   => value.toDouble.asRight
        case Avro.Int(value, _)  => value.toDouble.asRight
        case Avro.Long(value, _) => value.toDouble.asRight
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Double", "Double"))
      }
    )

  /**
    * @group General
    */
  implicit final def either[A, B](
    implicit codecA: Codec[A],
    codecB: Codec[B]
  ): Codec[Either[A, B]] =
    Codec.union(alt => alt[Left[A, B]] |+| alt[Right[A, B]])

  /**
    * Returns the result of encoding the specified value.
    *
    * @group Utilities
    */
  final def encode[A](a: A)(implicit codec: Codec[A]): Either[AvroError, Avro] =
    codec.encode(a)

  /**
    * Returns a new enum [[Codec]] for type `A`.
    *
    * @group Create
    */
  final def enumeration[A](
    name: String,
    namespace: String,
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A],
    default: Option[A] = None,
    doc: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    props: Props = Props.empty
  ): Codec.Aux[A, Avro.Enum] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val schema = AvroError.catchNonFatal {
      props.toChain.map { props =>
        val schema =
          Schema.createEnum(
            name,
            doc.orNull,
            namespace,
            symbols.asJava,
            default.map(encode).orNull
          )

        aliases.foreach(schema.addAlias)

        props.foldLeft(()) {
          case ((), (name, value)) =>
            schema.addProp(name, value)
        }

        schema
      }
    }
    Codec.instance(
      schema,
      a => {
        val symbol = encode(a)
        if (symbols.contains(symbol))
          schema.map(Avro.Enum(symbol, _))
        else
          Left(AvroError.encodeSymbolNotInSchema(symbol, symbols, typeName))
      }, {
        case Avro.Enum(symbol, _) =>
          if (symbols.contains(symbol))
            decode(symbol)
          else
            default.toRight(AvroError.decodeSymbolNotInSchema(symbol, symbols, typeName))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "GenericEnumSymbol", typeName))
      }
    )
  }

  @deprecated("Use Codec.enumeration - enum is a keyword in Scala 3", "1.3.0")
  final def `enum`[A](
    name: String,
    namespace: String,
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A],
    default: Option[A] = None,
    doc: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    props: Props = Props.empty
  ): Codec.Aux[A, Avro.Enum] =
    enumeration(name, namespace, symbols, encode, decode, default, doc, aliases, props)

  /**
    * Returns a new fixed [[Codec]] for type `A`.
    *
    * When encoding, bytes are zero-padded to the specified size.
    * Zero-padding is applied at the end, and will remain in the
    * input to `decode`. Encoding checks to ensure the size is
    * not exceeded, while decoding ensures the exact size.
    *
    * @group Create
    */
  final def fixed[A](
    name: String,
    namespace: String,
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A],
    doc: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    props: Props = Props.empty
  ): Codec.Aux[A, Avro.Fixed] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val schema = AvroError.catchNonFatal {
      props.toChain.map { props =>
        val schema =
          SchemaFactory.fixed(name, namespace, aliases.toArray, doc.orNull, size)

        props.foldLeft(()) {
          case ((), (name, value)) =>
            schema.addProp(name, value)
        }

        schema
      }
    }
    Codec
      .instance(
        schema,
        a => {
          val bytes = encode(a)
          if (bytes.length <= size) {
            schema.map(Avro.Fixed(bytes, _))
          } else {
            Left(AvroError.encodeExceedsFixedSize(bytes.length, size, typeName))
          }
        }, {
          case Avro.Fixed(bytes, _) =>
            if (bytes.length == size) {
              decode(bytes)
            } else {
              Left {
                AvroError.decodeNotEqualFixedSize(
                  bytes.length,
                  size,
                  typeName
                )
              }
            }

          case other =>
            Left(AvroError.decodeUnexpectedType(other, "GenericFixed", typeName))
        }
      )
  }

  /**
    * @group General
    */
  implicit final val float: Codec.Aux[Float, Avro.Float] =
    Codec.instance(
      Right(SchemaBuilder.builder().floatType()),
      Avro.Float(_).asRight, {
        case Avro.Float(float) =>
          Right(float)
        case Avro.Long(long, _) => Right(long.toFloat)
        case Avro.Int(int, _)   => Right(int.toFloat)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Float", "Float"))

      }
    )

  /**
    * Returns the result of decoding the specified
    * Avro binary to the specified type.
    *
    * @group Utilities
    */
  final def fromBinary[A](bytes: Array[Byte], writerSchema: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    AvroError.catchNonFatal {
      val bais = new ByteArrayInputStream(bytes)
      val decoder = DecoderFactory.get.binaryDecoder(bais, null)
      val value = new GenericDatumReader[Any](writerSchema).read(null, decoder)
      Avro.fromJava(value, writerSchema).flatMap(codec.decode(_))
    }

  /**
    * Returns the result of decoding the specified
    * Avro JSON to the specified type.
    *
    * @group Utilities
    */
  final def fromJson[A](json: String, writerSchema: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    AvroError.catchNonFatal {
      val bais = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
      val decoder = DecoderFactory.get.jsonDecoder(writerSchema, bais)
      val value = new GenericDatumReader[Any](writerSchema).read(null, decoder)
      Avro.fromJava(value, writerSchema).flatMap(codec.decode(_))
    }

  /**
    * Returns a new [[Codec]] instance using the specified
    * `Schema`, and encode and decode functions.
    *
    * @group Create
    */
  final def instance[A, T0 <: Avro](
    schema: Either[AvroError, Schema],
    encode: A => Either[AvroError, T0],
    decode: Avro => Either[AvroError, A]
  ): Codec.Aux[A, T0] = {
    val _schema = schema
    val _encode = encode
    val _decode = decode

    new Codec[A] {
      type Repr = T0

      override final val schema: Either[AvroError, Schema] =
        _schema

      override final def encode(a: A): Either[AvroError, T0] =
        _encode(a)

      override final def decode(value: Avro): Either[AvroError, A] =
        _decode(value)

      override final def toString: String =
        schema match {
          case Right(schema) => s"Codec(${schema.toString(true)})"
          case Left(error)   => error.toString()
        }
    }
  }

  /**
    * @group JavaTime
    */
  implicit final val instant: Codec.Aux[Instant, Avro.Long] =
    Codec.instance(
      Right(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())),
      instant => Right(Avro.Long(instant.toEpochMilli, Some(LogicalTypes.timestampMillis()))), {
        case Avro.Long(long, lt) =>
          if (lt == Some(LogicalTypes.timestampMillis())) {
            Right(Instant.ofEpochMilli(long))
          } else Left(AvroError.decodeUnexpectedLogicalType(lt.orNull, "Instant"))
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Long", "Instant"))
      }
    )

  /**
    * @group General
    */
  implicit final val int: Codec.Aux[Int, Avro.Int] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      Avro.Int(_, None).asRight, {
        case Avro.Int(integer, _) =>
          Right(integer)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Int", "Int"))
      }
    )

  /**
    * @group General
    */
  implicit final def left[A, B](implicit codec: Codec[A]): Codec[Left[A, B]] =
    codec.imap(Left[A, B](_))(_.value)

  /**
    * @group Collection
    */
  implicit final def list[A](implicit codec: Codec[A]): Codec.Aux[List[A], Avro.Array[codec.Repr]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode(_)).map(items => Avro.Array(items.toVector)), {
        case Avro.Array(items) =>
          items.toList.traverse(codec.decode)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "List"))
      }
    )

  /**
    * @group JavaTime
    */
  implicit final val localDate: Codec.Aux[LocalDate, Avro.Int] =
    Codec.instance(
      Right(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())),
      localDate => Right(Avro.Int(localDate.toEpochDay.toInt, Some(LogicalTypes.date()))), {
        case Avro.Int(int, lt) =>
          if (lt == Some(LogicalTypes.date())) {
            Right(LocalDate.ofEpochDay(int.toLong))
          } else Left(AvroError.decodeUnexpectedLogicalType(lt.orNull, "LocalDate"))
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Integer", "LocalDate"))
      }
    )

  /**
    * @group General
    */
  implicit final val long: Codec.Aux[Long, Avro.Long] =
    Codec.instance(
      Right(SchemaBuilder.builder().longType()),
      Avro.Long(_, None).asRight, {
        case Avro.Long(long, _) =>
          Right(long)
        case Avro.Int(int, _) => Right(int.toLong)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Long", "Long"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def map[A](
    implicit codec: Codec[A]
  ): Codec.Aux[Map[String, A], Avro.Map[codec.Repr]] =
    Codec.instance(
      codec.schema.map(Schema.createMap),
      _.toList
        .traverse {
          case (key, value) =>
            codec
              .encode(value)
              .tupleLeft(key)
        }
        .map(kvs => Avro.Map(kvs.toMap)), {
        case Avro.Map(keyvalues) =>
          keyvalues.toList
            .traverse {
              case (key, value) => codec.decode(value).tupleLeft(key)
            }
            .map(_.toMap)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "java.util.Map", "Map"))
      }
    )

  /**
    * @group General
    */
  implicit final val none: Codec.Aux[None.type, Avro.Null] =
    Codec.instance(
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(Avro.Null), {
        case Avro.Null => Right(None)
        case value     => Left(AvroError.decodeUnexpectedType(value, "null", "None"))

      }
    )

  implicit final def set[A](implicit codec: Codec[A]): Codec.Aux[Set[A], Avro.Array[codec.Repr]] =
    Codec
      .list[A]
      .imap(_.toSet)(_.toList)
      .withDecodingTypeName("Set")

  /**
    * @group Cats
    */
  implicit final def nonEmptyChain[A](
    implicit codec: Codec[A]
  ): Codec.Aux[NonEmptyChain[A], Avro.Array[codec.Repr]] =
    Codec
      .chain[A]
      .imapError(
        NonEmptyChain.fromChain(_).toRight(AvroError.decodeEmptyCollection("NonEmptyChain"))
      )(_.toChain)
      .withDecodingTypeName("NonEmptyChain")

  /**
    * @group Cats
    */
  implicit final def nonEmptyList[A](
    implicit codec: Codec[A]
  ): Codec.Aux[NonEmptyList[A], Avro.Array[codec.Repr]] =
    Codec
      .list[A]
      .imapError(
        NonEmptyList.fromList(_).toRight(AvroError.decodeEmptyCollection("NonEmptyList"))
      )(_.toList)
      .withDecodingTypeName("NonEmptyList")

  /**
    * @group Cats
    */
  implicit final def nonEmptySet[A](
    implicit codec: Codec[A],
    ordering: Ordering[A]
  ): Codec.Aux[NonEmptySet[A], Avro.Array[codec.Repr]] =
    Codec
      .list[A]
      .imapError(
        list =>
          NonEmptySet
            .fromSet(SortedSet(list: _*))
            .toRight(AvroError.decodeEmptyCollection("NonEmptySet"))
      )(_.toList)
      .withDecodingTypeName("NonEmptySet")

  /**
    * @group Cats
    */
  implicit final def nonEmptyVector[A](
    implicit codec: Codec[A]
  ): Codec.Aux[NonEmptyVector[A], Avro.Array[codec.Repr]] =
    Codec
      .vector[A]
      .imapError(
        NonEmptyVector.fromVector(_).toRight(AvroError.decodeEmptyCollection("NonEmptyVector"))
      )(_.toVector)
      .withDecodingTypeName("NonEmptyVector")

  /**
    * @group General
    */
  implicit final def option[A](implicit codec: Codec[A]): Codec[Option[A]] =
    Codec.union(alt => alt[None.type] |+| alt[Some[A]])

  /**
    * Returns a new record [[Codec]] for type `A`.
    *
    * @group Create
    */
  final def record[A](
    name: String,
    namespace: String,
    doc: Option[String] = None,
    aliases: Seq[String] = Seq.empty,
    props: Props = Props.empty
  )(f: FieldBuilder[A] => FreeApplicative[Field[A, *], A]): Codec.Aux[A, Avro.Record] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val free = f(FieldBuilder.instance)
    val schema = AvroError.catchNonFatal {
      val fields =
        free.analyze {
          new (Field[A, *] ~> λ[a => Either[AvroError, Chain[Schema.Field]]]) {
            def apply[B](field: Field[A, B]) =
              field.codec.schema.flatMap { schema =>
                field.props.toChain
                  .flatMap { props =>
                    field.default
                      .traverse(field.codec.encode(_).flatMap(Avro.toJava))
                      .map { default =>
                        Chain.one {
                          val schemaField =
                            new Schema.Field(
                              field.name,
                              schema,
                              field.doc.orNull,
                              adaptForSchema {
                                default.map {
                                  case null  => Schema.Field.NULL_DEFAULT_VALUE
                                  case other => other
                                }.orNull
                              },
                              field.order.getOrElse(Schema.Field.Order.ASCENDING)
                            )

                          field.aliases.foreach(schemaField.addAlias)

                          props.foldLeft(()) {
                            case ((), (name, value)) =>
                              schemaField.addProp(name, value)
                          }

                          schemaField
                        }
                      }
                  }
              }
          }
        }

      fields.flatMap { fields =>
        props.toChain.map { props =>
          val record =
            Schema.createRecord(
              name,
              doc.orNull,
              namespace,
              false,
              fields.toList.asJava
            )

          aliases.foreach(record.addAlias)

          props.foldLeft(()) {
            case ((), (name, value)) =>
              record.addProp(name, value)
          }

          record
        }
      }
    }
    Codec.instance(
      schema,
      a =>
        schema.flatMap { schema =>
          val fields =
            free.analyze {
              new (Field[A, *] ~> λ[a => Either[AvroError, Chain[(String, Avro)]]]) {
                def apply[B](field: Field[A, B]) =
                  field.codec.encode(field.access(a)).map(result => Chain.one((field.name, result)))
              }
            }

          fields.map { values =>
            Avro.Record(values.toList.toMap, schema)
          }
        }, {
        case Avro.Record(fields, _) =>
          free.foldMap {
            new (Field[A, *] ~> Either[AvroError, *]) {
              def apply[B](field: Field[A, B]) = {
                val value = fields.get(field.name)
                value match {
                  case Some(value) =>
                    field.codec.decode(value)
                  case None =>
                    field.default.toRight {
                      AvroError.decodeMissingRecordField(field.name, typeName)
                    }
                }
              }
            }
          }

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
      }
    )
  }

  /**
    * @group General
    */
  implicit final def right[A, B](implicit codec: Codec[B]): Codec[Right[A, B]] =
    codec.imap(Right[A, B](_))(_.value)

  /**
    * @group Collection
    */
  implicit final def seq[A](implicit codec: Codec[A]): Codec[Seq[A]] =
    Codec
      .list[A]
      .imap[Seq[A]](_.toSeq)(_.toList)
      .withDecodingTypeName("Seq")

  implicit final val short: Codec[Short] = {
    val min: Int = Short.MinValue.toInt
    val max: Int = Short.MaxValue.toInt
    Codec.int
      .imapError { integer =>
        if (min <= integer && integer <= max)
          Right(integer.toShort)
        else Left(AvroError.unexpectedShort(integer))
      }(_.toInt)
      .withDecodingTypeName("Short")
  }

  /**
    * @group General
    */
  implicit final def some[A](implicit codec: Codec[A]): Codec[Some[A]] =
    codec.imap(Some(_))(_.value)

  /**
    * @group General
    */
  implicit final val string: Codec.Aux[String, Avro.String] =
    Codec.instance(
      Right(SchemaBuilder.builder().stringType()),
      Avro.String(_, None).asRight, {
        case Avro.String(string, _) =>
          Right(string)
        case Avro.Bytes(bytes, _) =>
          AvroError.catchNonFatal(Right(StandardCharsets.UTF_8.decode(bytes).toString))
        case other =>
          Left {
            AvroError
              .decodeUnexpectedTypes(other, NonEmptyList.of("String", "Utf8"), "String")
          }
      }
    )

  /**
    * Returns the result of encoding the specified
    * value to Avro binary.
    *
    * @group Utilities
    */
  final def toBinary[A](a: A)(implicit codec: Codec[A]): Either[AvroError, Array[Byte]] =
    codec.schema.flatMap { schema =>
      codec.encode(a).flatMap(Avro.toJava).flatMap { encoded =>
        AvroError.catchNonFatal {
          val baos = new ByteArrayOutputStream
          val encoder = EncoderFactory.get.binaryEncoder(baos, null)
          new GenericDatumWriter[Any](schema).write(encoded, encoder)
          encoder.flush()
          Right(baos.toByteArray)
        }
      }
    }

  /**
    * Returns the result of encoding the specified
    * value to Avro JSON.
    *
    * @group Utilities
    */
  final def toJson[A](a: A)(implicit codec: Codec[A]): Either[AvroError, String] =
    codec.schema.flatMap { schema =>
      codec.encode(a).flatMap(Avro.toJava).flatMap { encoded =>
        AvroError.catchNonFatal {
          val baos = new ByteArrayOutputStream
          val encoder = EncoderFactory.get.jsonEncoder(schema, baos)
          new GenericDatumWriter[Any](schema).write(encoded, encoder)
          encoder.flush()
          Right(new String(baos.toByteArray, StandardCharsets.UTF_8))
        }
      }
    }

  /**
    * Returns a new union [[Codec]] for type `A`.
    *
    * @group Create
    */
  final def union[A](f: AltBuilder[A] => Chain[Alt[A]]): Codec[A] = {
    val alts = f(AltBuilder.instance)
    val schema = AvroError.catchNonFatal {
      alts.toList
        .traverse(_.codec.schema)
        .map(schemas => Schema.createUnion(schemas.asJava))
    }

    Codec.instance[A, Avro](
      schema,
      a =>
        alts
          .foldMapK { alt =>
            alt.prism.getOption(a).map(alt.codec.encode(_))
          }
          .getOrElse {
            Left(AvroError.encodeExhaustedAlternatives(a, None))
          },
      value =>
        alts
          .collectFirstSome { alt =>
            alt.codec
              .decode(value)
              .map(alt.prism.reverseGet)
              .toOption
          }
          .toRight {
            AvroError.decodeExhaustedAlternatives(value, None)
          }
    )
  }

  /**
    * @group General
    */
  implicit final val unit: Codec.Aux[Unit, Avro.Null] =
    Codec.instance(
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(Avro.Null), {
        case Avro.Null => Right(())
        case value     => Left(AvroError.decodeUnexpectedType(value, "null", "Unit"))
      }
    )

  /**
    * @group JavaUtil
    */
  implicit final val uuid: Codec.Aux[UUID, Avro.String] =
    Codec.instance(
      Right(LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType())),
      uuid => Right(Avro.String(uuid.toString(), Some(LogicalTypes.uuid()))), {
        case Avro.String(s, lt) =>
          if (lt == Some(LogicalTypes.uuid())) {
            AvroError.catchNonFatal {
              Right(UUID.fromString(s))
            }
          } else Left(AvroError.decodeUnexpectedLogicalType(lt.orNull, "UUID"))
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Utf8", "UUID"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def vector[A](
    implicit codec: Codec[A]
  ): Codec.Aux[Vector[A], Avro.Array[codec.Repr]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode(_)).map(items => Avro.Array(items)), {
        case Avro.Array(elems) =>
          elems.traverse(codec.decode(_))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "Vector"))
      }
    )

  /**
    * @group Cats
    */
  implicit final val codecInvariant: Invariant[Codec] =
    new Invariant[Codec] {
      override final def imap[A, B](codec: Codec[A])(f: A => B)(g: B => A): Codec[B] =
        codec.imap(f)(g)
    }

  /**
    * @group Cats
    */
  implicit final def codecShow[A]: Show[Codec[A]] =
    Show.fromToString

  implicit final def codecShowAux[A, Repr <: Avro]: Show[Codec.Aux[A, Repr]] =
    Show.fromToString

  /**
    * @group Create
    */
  sealed abstract class Alt[A] {
    type B

    def codec: Codec[B]

    def prism: Prism[A, B]
  }

  private[vulcan] object Alt {
    final def apply[A, B](
      codec: Codec[B],
      prism: Prism[A, B]
    ): Alt[A] = {
      type B0 = B
      val _codec = codec
      val _prism = prism

      new Alt[A] {
        override final type B = B0
        override final val codec: Codec[B] = _codec
        override final val prism: Prism[A, B] = _prism
      }
    }
  }

  /**
    * @group Create
    */
  sealed abstract class AltBuilder[A] {
    def apply[B](
      implicit codec: Codec[B],
      prism: Prism[A, B]
    ): Chain[Alt[A]]
  }

  private[vulcan] object AltBuilder {
    private[this] final val Instance: AltBuilder[Avro] =
      new AltBuilder[Avro] {
        override final def apply[B](
          implicit codec: Codec[B],
          prism: Prism[Avro, B]
        ): Chain[Alt[Avro]] =
          Chain.one(Alt(codec, prism))

        override final def toString: String =
          "AltBuilder"
      }

    final def instance[A]: AltBuilder[A] =
      Instance.asInstanceOf[AltBuilder[A]]
  }

  /**
    * @group Create
    */
  sealed abstract class Field[A, B] {
    def name: String

    def access: A => B

    def codec: Codec[B]

    def doc: Option[String]

    def default: Option[B]

    def order: Option[Schema.Field.Order]

    def aliases: Seq[String]

    def props: Props
  }

  private[vulcan] object Field {
    final def apply[A, B](
      name: String,
      access: A => B,
      codec: Codec[B],
      doc: Option[String],
      default: Option[B],
      order: Option[Schema.Field.Order],
      aliases: Seq[String],
      props: Props
    ): Field[A, B] = {
      val _name = name
      val _access = access
      val _codec = codec
      val _doc = doc
      val _default = default
      val _order = order
      val _aliases = aliases
      val _props = props

      new Field[A, B] {
        override final val name: String = _name
        override final val access: A => B = _access
        override final val codec: Codec[B] = _codec
        override final val doc: Option[String] = _doc
        override final val default: Option[B] = _default
        override final val order: Option[Schema.Field.Order] = _order
        override final val aliases: Seq[String] = _aliases
        override final val props: Props = _props
      }
    }
  }

  /**
    * @group Create
    */
  sealed abstract class FieldBuilder[A] {
    def apply[B](
      name: String,
      access: A => B,
      doc: Option[String] = None,
      default: Option[B] = None,
      order: Option[Schema.Field.Order] = None,
      aliases: Seq[String] = Seq.empty,
      props: Props = Props.empty
    )(implicit codec: Codec[B]): FreeApplicative[Field[A, *], B]
  }

  private[vulcan] object FieldBuilder {
    private[this] final val Instance: FieldBuilder[Avro] =
      new FieldBuilder[Avro] {
        override final def apply[B](
          name: String,
          access: Avro => B,
          doc: Option[String],
          default: Option[B],
          order: Option[Schema.Field.Order],
          aliases: Seq[String],
          props: Props
        )(implicit codec: Codec[B]): FreeApplicative[Field[Avro, *], B] =
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

    final def instance[A]: FieldBuilder[A] =
      Instance.asInstanceOf[FieldBuilder[A]]
  }
}
