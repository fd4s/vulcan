/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.{Invariant, Show, ~>}
import cats.data.{Chain, NonEmptyChain, NonEmptyList, NonEmptySet, NonEmptyVector}
import cats.free.FreeApplicative
import cats.implicits._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalTime}
import java.util.concurrent.TimeUnit
import java.util.UUID
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8

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
  type Repr

  /** The schema or an error if the schema could not be generated. */
  def schema: Either[AvroError, Schema]

  /** Attempts to encode the specified value using the provided schema. */
  def encode(a: A): Either[AvroError, Repr]

  /** Attempts to decode the specified value using the provided schema. */
  def decode(value: Any, schema: Schema): Either[AvroError, A]

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    */
  final def imap[B](f: A => B)(g: B => A): Codec.Aux[Repr, B] =
    Codec.instance(
      schema,
      b => encode(g(b)),
      (a, schema) => decode(a, schema).map(f)
    )

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    *
    * Similar to [[Codec#imap]], except the mapping from
    * `A` to `B` might be unsuccessful.
    */
  final def imapError[B](f: A => Either[AvroError, B])(g: B => A): Codec.Aux[Repr, B] =
    Codec.instance(
      schema,
      b => encode(g(b)),
      (a, schema) => decode(a, schema).flatMap(f)
    )

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    *
    * Similar to [[Codec#imap]], except the mapping from
    * `A` to `B` might be unsuccessful.
    */
  final def imapTry[B](f: A => Try[B])(g: B => A): Codec.Aux[Repr, B] =
    imapError(f(_).toEither.leftMap(AvroError.fromThrowable))(g)

  private[vulcan] def withTypeName(typeName: String): Codec.Aux[Repr, A] =
    Codec.instance(
      schema,
      encode(_).leftMap(AvroError.errorEncodingFrom(typeName, _)),
      decode(_, _).leftMap(AvroError.errorDecodingTo(typeName, _))
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

  type Aux[Repr0, A] = Codec[A] {
    type Repr = Repr0
  }

  /**
    * Returns the [[Codec]] for the specified type.
    *
    * @group Utilities
    */
  final def apply[A](implicit codec: Codec[A]): codec.type = codec

  /**
    * @group General
    */
  implicit final val boolean: Codec.Aux[Boolean, Boolean] =
    Codec.instanceForTypes(
      "Boolean",
      "Boolean",
      Right(SchemaBuilder.builder().booleanType()),
      _.asRight,
      { case (boolean: Boolean, _) => Right(boolean) }
    )

  /**
    * @group General
    */
  implicit final lazy val byte: Codec.Aux[Int, Byte] = {
    Codec.int
      .imapError { integer =>
        if (integer.isValidByte) Right(integer.toByte)
        else Left(AvroError.unexpectedByte(integer))
      }(_.toInt)
      .withTypeName("Byte")
  }

  /**
    * @group General
    */
  implicit final val bytes: Codec.Aux[ByteBuffer, Array[Byte]] =
    Codec
      .instance[ByteBuffer, Array[Byte]](
        Right(SchemaBuilder.builder().bytesType()),
        ByteBuffer.wrap(_).asRight,
        (value, schema) => {
          schema.getType() match {
            case BYTES | STRING =>
              value match {
                case buffer: ByteBuffer => Right(buffer.array())
                case utf8: Utf8         => Right(utf8.getBytes)
                case string: String     => Right(string.getBytes(StandardCharsets.UTF_8))
                case other              => Left(AvroError.decodeUnexpectedType(other, "ByteBuffer"))
              }

            case schemaType =>
              Left {
                AvroError.decodeUnexpectedSchemaType(schemaType, BYTES)
              }
          }
        }
      )
      .withTypeName("Array[Byte]")

  /**
    * @group Cats
    */
  implicit final def chain[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], Chain[A]] =
    Codec.instanceForTypes(
      "Collection",
      "Chain",
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode(_)).map(_.asJava), {
        case (collection: java.util.Collection[_], schema) =>
          collection.asScala.toList
            .traverse(codec.decode(_, schema.getElementType()))
            .map(Chain.fromSeq)
      }
    )

  /**
    * @group General
    */
  implicit final val char: Codec.Aux[Utf8, Char] =
    Codec.instanceForTypes(
      "Utf8",
      "Char",
      Right(SchemaBuilder.builder().stringType()),
      char => Right(new Utf8(char.toString)), {
        case (utf8: Utf8, _) =>
          val string = utf8.toString
          if (string.length == 1) Right(string.charAt(0))
          else Left(AvroError.unexpectedChar(string.length))
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
  ): Codec.Aux[ByteBuffer, BigDecimal] = {
    val conversion = new Conversions.DecimalConversion()
    val logicalType = LogicalTypes.decimal(precision, scale)
    val schema = AvroError.catchNonFatal {
      Right {
        logicalType.addToSchema(SchemaBuilder.builder().bytesType())
      }
    }
    Codec.instanceForTypes[ByteBuffer, BigDecimal](
      "ByteBuffer",
      "BigDecimal",
      schema,
      bigDecimal =>
        if (bigDecimal.scale == scale) {
          if (bigDecimal.precision <= precision) {
            schema.map(conversion.toBytes(bigDecimal.underlying(), _, logicalType))
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
        case (buffer: ByteBuffer, schema) =>
          schema.getLogicalType() match {
            case decimal: LogicalTypes.Decimal =>
              val bigDecimal = BigDecimal(conversion.fromBytes(buffer, schema, decimal))
              if (bigDecimal.precision <= decimal.getPrecision()) {
                Right(bigDecimal)
              } else
                Left {
                  AvroError
                    .decodeDecimalPrecisionExceeded(
                      bigDecimal.precision,
                      decimal.getPrecision()
                    )
                }
            case logicalType =>
              Left(AvroError.decodeUnexpectedLogicalType(logicalType))
          }

      }
    )
  }

  /**
    * Returns the result of decoding the specified value
    * to the specified type.
    *
    * @group Utilities
    */
  final def decode[A](value: Any)(implicit codec: Codec[A]): Either[AvroError, A] =
    codec.schema.flatMap(codec.decode(value, _))

  /**
    * @group General
    */
  implicit final val double: Codec.Aux[Double, Double] =
    Codec
      .instance[Double, Double](
        Right(SchemaBuilder.builder().doubleType()),
        _.asRight,
        (value, schema) => {
          schema.getType() match {
            case DOUBLE | FLOAT | INT | LONG =>
              value match {
                case double: Double => Right(double)
                case float: Float   => Right(float.toDouble)
                case int: Integer   => Right(int.toDouble)
                case long: Long     => Right(long.toDouble)
                case other =>
                  Left(
                    AvroError.decodeUnexpectedTypes(
                      other,
                      NonEmptyList.of("Double", "Float", "Integer", "Long")
                    )
                  )
              }
            case schemaType =>
              Left {
                AvroError
                  .decodeUnexpectedSchemaType(schemaType, DOUBLE)
              }
          }
        }
      )
      .withTypeName("Double")

  /**
    * @group General
    */
  implicit final def either[A, B](
    implicit codecA: Codec[A],
    codecB: Codec[B]
  ): Codec.Aux[Any, Either[A, B]] =
    Codec
      .union[Either[A, B]](alt => alt[Left[A, B]] |+| alt[Right[A, B]])
      .withTypeName("Either")

  /**
    * Returns the result of encoding the specified value.
    *
    * @group Utilities
    */
  final def encode[A](a: A)(implicit codec: Codec[A]): Either[AvroError, codec.Repr] =
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
  ): Codec.Aux[GenericData.EnumSymbol, A] = {
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
    Codec.instanceForTypes(
      "GenericEnumSymbol",
      typeName,
      schema,
      a => {
        val symbol = encode(a)
        if (symbols.contains(symbol))
          schema.map(new GenericData.EnumSymbol(_, symbol))
        else
          Left(AvroError.encodeSymbolNotInSchema(symbol, symbols))
      }, {
        case (genericEnum: GenericEnumSymbol[_], _) =>
          val symbol = genericEnum.toString()

          if (symbols.contains(symbol))
            decode(symbol)
          else
            default.toRight(AvroError.decodeSymbolNotInSchema(symbol, symbols))
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
  ): Codec.Aux[GenericData.EnumSymbol, A] =
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
  ): Codec.Aux[GenericFixed, A] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val schema = AvroError.catchNonFatal {
      props.toChain.map { props =>
        val schema =
          SchemaBuilder
            .builder(namespace)
            .fixed(name)
            .aliases(aliases: _*)
            .doc(doc.orNull)
            .size(size)

        props.foldLeft(()) {
          case ((), (name, value)) =>
            schema.addProp(name, value)
        }

        schema
      }
    }
    Codec
      .instanceForTypes(
        "GenericFixed",
        typeName,
        schema,
        a => {
          val bytes = encode(a)
          if (bytes.length <= size) {
            val buffer = ByteBuffer.allocate(size).put(bytes)
            schema.map(new GenericData.Fixed(_, buffer.array()))
          } else {
            Left(AvroError.encodeExceedsFixedSize(bytes.length, size))
          }
        }, {
          case (fixed: GenericFixed, schema) =>
            val bytes = fixed.bytes()
            if (bytes.length == schema.getFixedSize()) {
              decode(bytes)
            } else {
              Left {
                AvroError.decodeNotEqualFixedSize(
                  bytes.length,
                  schema.getFixedSize()
                )
              }
            }
        }
      )
  }

  /**
    * @group General
    */
  implicit final val float: Codec.Aux[Float, Float] =
    Codec
      .instance[Float, Float](
        Right(SchemaBuilder.builder().floatType()),
        _.asRight,
        (value, schema) => {
          schema.getType() match {
            case FLOAT | INT | LONG =>
              value match {
                case float: Float => Right(float)
                case int: Int     => Right(int.toFloat)
                case long: Long   => Right(long.toFloat)
                case other        => Left(AvroError.decodeUnexpectedType(other, "Float"))
              }

            case schemaType =>
              Left {
                AvroError
                  .decodeUnexpectedSchemaType(schemaType, FLOAT)
              }
          }
        }
      )
      .withTypeName("Float")

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
      codec.decode(value, writerSchema)
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
      codec.decode(value, writerSchema)
    }

  /**
    * Returns a new [[Codec]] instance using the specified
    * `Schema`, and encode and decode functions.
    *
    * @group Create
    */
  final def instance[Repr0, A](
    schema: Either[AvroError, Schema],
    encode: A => Either[AvroError, Repr0],
    decode: (Any, Schema) => Either[AvroError, A]
  ): Codec.Aux[Repr0, A] = {
    val _schema = schema
    val _encode = encode
    val _decode = decode

    new Codec[A] {
      type Repr = Repr0

      override final val schema: Either[AvroError, Schema] =
        _schema

      override final def encode(a: A): Either[AvroError, Repr] =
        _encode(a)

      override final def decode(value: Any, schema: Schema): Either[AvroError, A] =
        _decode(value, schema)

      override final def toString: String =
        schema match {
          case Right(schema) => s"Codec(${schema.toString(true)})"
          case Left(error)   => error.toString()
        }
    }
  }

  private[vulcan] final def instanceForTypes[Repr, A](
    expectedValueType: String,
    typeName: String,
    schema: Either[AvroError, Schema],
    encode: A => Either[AvroError, Repr],
    decode: PartialFunction[(Any, Schema), Either[AvroError, A]]
  ): Codec.Aux[Repr, A] =
    instance(
      schema,
      encode(_).leftMap(AvroError.errorEncodingFrom(typeName, _)),
      (value, writerSchema) =>
        schema
          .flatMap { readerSchema =>
            val schemaType = readerSchema.getType()
            if (writerSchema.getType() == schemaType)
              decode
                .lift((value, writerSchema))
                .getOrElse(
                  Left(AvroError.decodeUnexpectedType(value, expectedValueType))
                )
            else
              Left {
                AvroError
                  .decodeUnexpectedSchemaType(writerSchema.getType(), schemaType)
              }
          }
          .leftMap(AvroError.errorDecodingTo(typeName, _))
    )

  /**
    * @group JavaTime
    */
  implicit final val instant: Codec.Aux[Long, Instant] =
    Codec.instanceForTypes(
      "Long",
      "Instant",
      Right(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())),
      instant => Right(instant.toEpochMilli), {
        case (long: Long, schema) =>
          val logicalType = schema.getLogicalType()
          if (logicalType == LogicalTypes.timestampMillis()) {
            Right(Instant.ofEpochMilli(long))
          } else Left(AvroError.decodeUnexpectedLogicalType(logicalType))
      }
    )

  /**
    * @group General
    */
  implicit final val int: Codec.Aux[Int, Int] =
    Codec.instanceForTypes(
      "Int",
      "Int",
      Right(SchemaBuilder.builder().intType()),
      _.asRight,
      { case (integer: Int, _) => Right(integer) }
    )

  /**
    * @group General
    */
  implicit final def left[A, B](implicit codec: Codec[A]): Codec.Aux[codec.Repr, Left[A, B]] =
    codec.imap(Left[A, B](_))(_.value)

  /**
    * @group Collection
    */
  implicit final def list[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], List[A]] =
    Codec.instanceForTypes(
      "Collection",
      "List",
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode(_)).map(_.asJava), {
        case (collection: java.util.Collection[_], schema) =>
          collection.asScala.toList.traverse(codec.decode(_, schema.getElementType()))
      }
    )

  /**
    * @group JavaTime
    */
  implicit final val localDate: Codec.Aux[Int, LocalDate] =
    Codec.instanceForTypes(
      "Integer",
      "LocalDate",
      Right(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())),
      localDate => Right(localDate.toEpochDay.toInt), {
        case (int: Int, schema) =>
          val logicalType = schema.getLogicalType()
          if (logicalType == LogicalTypes.date()) {
            Right(LocalDate.ofEpochDay(int.toLong))
          } else Left(AvroError.decodeUnexpectedLogicalType(logicalType))
      }
    )

  /**
    * @group JavaTime
    */
  final val localTimeMillis: Codec.Aux[Int, LocalTime] =
    Codec.instanceForTypes(
      "Integer",
      "LocalTime",
      Right(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType())), {
        localTime =>
          val millis = TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay())
          Right(millis.toInt)
      }, {
        case (int: Int, schema) =>
          val logicalType = schema.getLogicalType()
          if (logicalType == LogicalTypes.timeMillis()) {
            val nanos = TimeUnit.MILLISECONDS.toNanos(int.toLong)
            Right(LocalTime.ofNanoOfDay(nanos))
          } else Left(AvroError.decodeUnexpectedLogicalType(logicalType))
      }
    )

  /**
    * @group JavaTime
    */
  final val localTimeMicros: Codec.Aux[Long, LocalTime] =
    Codec.instanceForTypes(
      "Long",
      "LocalTime",
      Right(LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType())), {
        localTime =>
          val micros = TimeUnit.NANOSECONDS.toMicros(localTime.toNanoOfDay())
          Right(micros)
      }, {
        case (long: Long, schema) =>
          val logicalType = schema.getLogicalType()
          if (logicalType == LogicalTypes.timeMicros()) {
            val nanos = TimeUnit.MICROSECONDS.toNanos(long)
            Right(LocalTime.ofNanoOfDay(nanos))
          } else Left(AvroError.decodeUnexpectedLogicalType(logicalType))
      }
    )

  /**
    * @group General
    */
  implicit final val long: Codec.Aux[Long, Long] =
    Codec
      .instance[Long, Long](
        Right(SchemaBuilder.builder().longType()),
        _.asRight,
        (value, schema) => {
          schema.getType() match {
            case LONG | INT =>
              value match {
                case long: Long =>
                  Right(long)
                case int: Int =>
                  Right(int.toLong)
                case other =>
                  Left(AvroError.decodeUnexpectedType(other, "Long"))
              }

            case schemaType =>
              Left {
                AvroError.decodeUnexpectedSchemaType(schemaType, LONG)
              }
          }
        }
      )
      .withTypeName("Long")

  /**
    * @group Collection
    */
  implicit final def map[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.Map[Utf8, codec.Repr], Map[String, A]] =
    Codec.instanceForTypes(
      "java.util.Map",
      "Map",
      codec.schema.map(Schema.createMap),
      _.toList
        .traverse {
          case (key, value) =>
            codec
              .encode(value)
              .tupleLeft(new Utf8(key))
        }
        .map(_.toMap.asJava), {
        case (map: java.util.Map[_, _], schema) =>
          map.asScala.toList
            .traverse {
              case (key: Utf8, value) =>
                codec.decode(value, schema.getValueType()).tupleLeft(key.toString)
              case (key, _) => Left(AvroError.decodeUnexpectedMapKey(key))
            }
            .map(_.toMap)
      }
    )

  /**
    * @group General
    */
  implicit final val none: Codec.Aux[Null, None.type] =
    Codec.instanceForTypes(
      "null",
      "None",
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(null),
      { case (value, null) => Right(None) }
    )

  /**
    * @group Cats
    */
  implicit final def nonEmptyChain[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], NonEmptyChain[A]] =
    Codec
      .chain[A]
      .imapError(
        NonEmptyChain.fromChain(_).toRight(AvroError.decodeEmptyCollection)
      )(_.toChain)
      .withTypeName("NonEmptyChain")

  /**
    * @group Cats
    */
  implicit final def nonEmptyList[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], NonEmptyList[A]] =
    Codec
      .list[A]
      .imapError(
        NonEmptyList.fromList(_).toRight(AvroError.decodeEmptyCollection)
      )(_.toList)
      .withTypeName("NonEmptyList")

  /**
    * @group Cats
    */
  implicit final def nonEmptySet[A](
    implicit codec: Codec[A],
    ordering: Ordering[A]
  ): Codec.Aux[java.util.List[codec.Repr], NonEmptySet[A]] =
    Codec
      .list[A]
      .imapError(
        list =>
          NonEmptySet
            .fromSet(SortedSet(list: _*))
            .toRight(AvroError.decodeEmptyCollection)
      )(_.toList)
      .withTypeName("NonEmptySet")

  /**
    * @group Cats
    */
  implicit final def nonEmptyVector[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], NonEmptyVector[A]] =
    Codec
      .vector[A]
      .imapError(
        NonEmptyVector.fromVector(_).toRight(AvroError.decodeEmptyCollection)
      )(_.toVector)
      .withTypeName("NonEmptyVector")

  /**
    * @group General
    */
  implicit final def option[A](implicit codec: Codec[A]): Codec[Option[A]] =
    Codec
      .union[Option[A]](alt => alt[None.type] |+| alt[Some[A]])
      .withTypeName("Option")

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
  )(f: FieldBuilder[A] => FreeApplicative[Field[A, *], A]): Codec.Aux[GenericRecord, A] = {
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
                      .traverse(field.codec.encode(_))
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
    Codec
      .instanceForTypes[GenericRecord, A](
        "IndexedRecord",
        typeName,
        schema,
        a =>
          schema.flatMap { schema =>
            val fields =
              free.analyze {
                new (Field[A, *] ~> λ[a => Either[AvroError, Chain[(String, Any)]]]) {
                  def apply[B](field: Field[A, B]) =
                    field.codec
                      .encode(field.access(a))
                      .map(result => Chain.one((field.name, result)))
                }
              }

            fields.map { values =>
              val record = new GenericData.Record(schema)
              values.foldLeft(()) {
                case ((), (name, value)) => record.put(name, value)
              }
              record
            }
          }, {
          case (record: IndexedRecord, _) =>
            val recordSchema = record.getSchema()
            val recordFields = recordSchema.getFields()

            free.foldMap {
              new (Field[A, *] ~> Either[AvroError, *]) {
                def apply[B](field: Field[A, B]) = {
                  val schemaField = recordSchema.getField(field.name)
                  if (schemaField != null) {
                    val value = record.get(recordFields.indexOf(schemaField))
                    field.codec.decode(value, schemaField.schema())
                  } else {
                    field.default.toRight {
                      AvroError.decodeMissingRecordField(field.name)
                    }
                  }
                }
              }
            }
        }
      )
  }

  /**
    * @group General
    */
  implicit final def right[A, B](implicit codec: Codec[B]): Codec.Aux[codec.Repr, Right[A, B]] =
    codec.imap(Right[A, B](_))(_.value).withTypeName("Right")

  /**
    * @group Collection
    */
  implicit final def seq[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], Seq[A]] =
    Codec
      .list[A]
      .imap[Seq[A]](_.toSeq)(_.toList)
      .withTypeName("Seq")

  /**
    * @group Collection
    */
  implicit final def set[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], Set[A]] =
    Codec
      .list[A]
      .imap(_.toSet)(_.toList)
      .withTypeName("Set")

  /**
    * @group General
    */
  implicit final val short: Codec.Aux[Int, Short] = {
    Codec.int
      .imapError { integer =>
        if (integer.isValidShort) Right(integer.toShort)
        else Left(AvroError.unexpectedShort(integer))
      }(_.toInt)
      .withTypeName("Short")
  }

  /**
    * @group General
    */
  implicit final def some[A](implicit codec: Codec[A]): Codec.Aux[codec.Repr, Some[A]] =
    codec.imap(Some(_))(_.value).withTypeName("Some")

  /**
    * @group General
    */
  implicit final val string: Codec.Aux[Utf8, String] =
    Codec
      .instance[Utf8, String](
        Right(SchemaBuilder.builder().stringType()),
        new Utf8(_).asRight,
        (value, schema) => {
          schema.getType() match {
            case STRING | BYTES =>
              value match {
                case string: String => Right(string)
                case utf8: Utf8     => Right(utf8.toString())
                case bytes: ByteBuffer =>
                  AvroError.catchNonFatal(Right(StandardCharsets.UTF_8.decode(bytes).toString))
                case other =>
                  Left {
                    AvroError
                      .decodeUnexpectedTypes(other, NonEmptyList.of("String", "Utf8"))
                  }
              }

            case schemaType =>
              Left {
                AvroError.decodeUnexpectedSchemaType(schemaType, STRING)
              }
          }
        }
      )
      .withTypeName("String")

  /**
    * Returns the result of encoding the specified
    * value to Avro binary.
    *
    * @group Utilities
    */
  final def toBinary[A](a: A)(implicit codec: Codec[A]): Either[AvroError, Array[Byte]] =
    codec.schema.flatMap { schema =>
      codec.encode(a).flatMap { encoded =>
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
      codec.encode(a).flatMap { encoded =>
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
  final def union[A](f: AltBuilder[A] => Chain[Alt[A]]): Codec.Aux[Any, A] = {
    val alts = f(AltBuilder.instance)
    val schema = AvroError.catchNonFatal {
      alts.toList
        .traverse(_.codec.schema)
        .map(schemas => Schema.createUnion(schemas.asJava))
    }

    Codec.instance[Any, A](
      schema,
      a =>
        alts
          .foldMapK { alt =>
            alt.prism.getOption(a).map(alt.codec.encode(_))
          }
          .getOrElse {
            Left(AvroError.encodeExhaustedAlternatives(a))
          },
      (value, schema) => {
        val schemaTypes =
          schema.getType() match {
            case UNION => schema.getTypes.asScala
            case _     => Seq(schema)
          }

        value match {
          case container: GenericContainer =>
            val altName =
              container.getSchema.getName

            val altUnionSchema =
              schemaTypes
                .find(_.getName == altName)
                .toRight(AvroError.decodeMissingUnionSchema(altName))

            def altMatching =
              alts
                .find(_.codec.schema.exists(_.getName == altName))
                .toRight(AvroError.decodeMissingUnionAlternative(altName))

            altUnionSchema.flatMap { altSchema =>
              altMatching.flatMap { alt =>
                alt.codec
                  .decode(container, altSchema)
                  .map(alt.prism.reverseGet)
              }
            }

          case other =>
            alts
              .collectFirstSome { alt =>
                alt.codec.schema
                  .traverse { altSchema =>
                    val altName = altSchema.getName
                    schemaTypes
                      .find(_.getName == altName)
                      .flatMap { schema =>
                        alt.codec
                          .decode(other, schema)
                          .map(alt.prism.reverseGet)
                          .toOption
                      }
                  }
              }
              .getOrElse {
                Left(AvroError.decodeExhaustedAlternatives(other))
              }
        }
      }
    )
  }.withTypeName("union")

  /**
    * @group General
    */
  implicit final val unit: Codec.Aux[Null, Unit] =
    Codec.instanceForTypes(
      "null",
      "Unit",
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(null),
      { case (value, null) => Right(()) }
    )

  /**
    * @group JavaUtil
    */
  implicit final val uuid: Codec.Aux[Utf8, UUID] =
    Codec.instanceForTypes(
      "Utf8",
      "UUID",
      Right(LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType())),
      uuid => Right(new Utf8(uuid.toString())), {
        case (utf8: Utf8, schema) =>
          val logicalType = schema.getLogicalType()
          if (logicalType == LogicalTypes.uuid()) {
            AvroError.catchNonFatal {
              Right(UUID.fromString(utf8.toString()))
            }
          } else Left(AvroError.decodeUnexpectedLogicalType(logicalType))
      }
    )

  /**
    * @group Collection
    */
  implicit final def vector[A](
    implicit codec: Codec[A]
  ): Codec.Aux[java.util.List[codec.Repr], Vector[A]] =
    Codec.instanceForTypes(
      "Collection",
      "Vector",
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode(_)).map(_.asJava), {
        case (collection: java.util.Collection[_], schema) =>
          collection.asScala.toVector.traverse(codec.decode(_, schema.getElementType()))
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

  /**
    * @group Cats
    */
  implicit final def codecAuxShow[Repr, A]: Show[Codec.Aux[Repr, A]] =
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
    final def apply[A, B0](
      _codec: Codec[B0],
      _prism: Prism[A, B0]
    ): Alt[A] = new Alt[A] {
      override final type B = B0
      override final val codec: Codec[B] = _codec
      override final val prism: Prism[A, B] = _prism
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
    private[this] final val Instance: AltBuilder[Any] =
      new AltBuilder[Any] {
        override final def apply[B](
          implicit codec: Codec[B],
          prism: Prism[Any, B]
        ): Chain[Alt[Any]] =
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
    private[this] final val Instance: FieldBuilder[Any] =
      new FieldBuilder[Any] {
        override final def apply[B](
          name: String,
          access: Any => B,
          doc: Option[String],
          default: Option[B],
          order: Option[Schema.Field.Order],
          aliases: Seq[String],
          props: Props
        )(implicit codec: Codec[B]): FreeApplicative[Field[Any, *], B] = {
          val _name = name
          val _access = access
          val _codec = codec
          val _doc = doc
          val _default = default
          val _order = order
          val _aliases = aliases
          val _props = props

          FreeApplicative.lift {
            new Field[Any, B] {
              override val name: String = _name
              override val access: Any => B = _access
              override val codec: Codec[B] = _codec
              override val doc: Option[String] = _doc
              override val default: Option[B] = _default
              override val order: Option[Schema.Field.Order] = _order
              override val aliases: Seq[String] = _aliases
              override val props: Props = _props
            }
          }
        }

        override final def toString: String =
          "FieldBuilder"
      }

    final def instance[A]: FieldBuilder[A] =
      Instance.asInstanceOf[FieldBuilder[A]]
  }
}
