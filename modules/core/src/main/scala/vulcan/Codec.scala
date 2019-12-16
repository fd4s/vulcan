/*
 * Copyright 2019 OVO Energy Limited
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
import org.apache.avro.util.Utf8
import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedSet
import scala.reflect.runtime.universe.WeakTypeTag
import org.apache.avro.data.TimeConversions
import vulcan.internal.converters.collection._
import vulcan.internal.schema.adaptForSchema
import vulcan.internal.tags._

/**
  * Provides a schema, along with encoding and decoding functions
  * for a given type.
  */
@implicitNotFound(
  "could not find implicit Codec[${A}]; ensure no imports are missing or manually define an instance"
)
sealed abstract class Codec[A] {

  /** The schema or an error if the schema could not be generated. */
  def schema: Either[AvroError, Schema]

  /** Attempts to encode the specified value using the provided schema. */
  def encode(a: A): Either[AvroError, Any]

  /** Attempts to decode the specified value using the provided schema. */
  def decode(value: Any): Either[AvroError, A]

  /**
    * Returns a new [[Codec]] which uses this [[Codec]]
    * for encoding and decoding, mapping back-and-forth
    * between types `A` and `B`.
    */
  final def imap[B](f: A => B)(g: B => A): Codec[B] =
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
  final def imapError[B](f: A => Either[AvroError, B])(g: B => A): Codec[B] =
    Codec.instance(
      schema,
      b => encode(g(b)),
      a => decode(a).flatMap(f)
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
final object Codec {
  GenericData.get.addLogicalTypeConversion(new TimeConversions.DateConversion())
  GenericData.get.addLogicalTypeConversion(new Conversions.DecimalConversion())
  GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion)
  GenericData.get.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion)
  GenericData.get.addLogicalTypeConversion(new Conversions.UUIDConversion)

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
  implicit final val boolean: Codec[Boolean] =
    Codec.instance(
      Right(SchemaBuilder.builder().booleanType()),
      java.lang.Boolean.valueOf(_).asRight, {
        case boolean: java.lang.Boolean =>
          Right(boolean)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Boolean", "Boolean"))
      }
    )

  /**
    * @group General
    */
  implicit final val byte: Codec[Byte] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      byte => Right(java.lang.Integer.valueOf(byte.toInt)), {
        case integer: java.lang.Integer =>
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
  implicit final val bytes: Codec[Array[Byte]] =
    Codec.instance(
      Right(SchemaBuilder.builder().bytesType()),
      ByteBuffer.wrap(_).asRight, {
        case buffer: ByteBuffer =>
          Right(buffer.array())
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "ByteBuffer", "Array[Byte]"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def chain[A](implicit codec: Codec[A]): Codec[Chain[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList
            .traverse(codec.decode)
            .map(Chain.fromSeq)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "Chain"))
      }
    )

  /**
    * @group General
    */
  implicit final val char: Codec[Char] =
    Codec.instance(
      Right(SchemaBuilder.builder().stringType()),
      char => Right(new Utf8(char.toString)), {
        case utf8: Utf8 =>
          val string = utf8.toString
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
  ): Codec[BigDecimal] = {
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
            Right(bigDecimal.underlying())
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
        case bigDecimal: java.math.BigDecimal =>
          if (bigDecimal.scale == scale) {
            if (bigDecimal.precision <= precision)
              Right(BigDecimal(bigDecimal))
            else
              Left {
                AvroError
                  .decodeDecimalPrecisionExceeded(
                    bigDecimal.precision,
                    precision
                  )
              }
          } else {
            Left(
              AvroError(
                "Cannot decode decimal with scale " + bigDecimal.scale + " as scale " + scale
              )
            )
          }
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "java.math.BigDecimal", "BigDecimal"))
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
    codec.decode(value)

  /**
    * Returns an enum [[Codec]] for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using type tags.
    *
    * @group Derive
    */
  final def deriveEnum[A](
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec[A] =
    Codec.enum(
      name = nameFrom(tag),
      symbols = symbols,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )

  /**
    * Returns a fixed [[Codec]] for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using type tags.
    *
    * @group Derive
    */
  final def deriveFixed[A](
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec[A] =
    Codec.fixed(
      name = nameFrom(tag),
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )

  /**
    * @group General
    */
  implicit final val double: Codec[Double] =
    Codec.instance(
      Right(SchemaBuilder.builder().doubleType()),
      java.lang.Double.valueOf(_).asRight, {
        case double: java.lang.Double =>
          Right(double)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Double", "Double"))
      }
    )

  /**
    * Returns the result of encoding the specified value.
    *
    * @group Utilities
    */
  final def encode[A](a: A)(implicit codec: Codec[A]): Either[AvroError, Any] =
    codec.encode(a)

  /**
    * Returns a new enum [[Codec]] for type `A`.
    *
    * @group Create
    */
  final def enum[A](
    name: String,
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A],
    namespace: String,
    aliases: Seq[String] = Seq.empty,
    doc: Option[String] = None,
    default: Option[A] = None,
    props: Props = Props.empty
  ): Codec[A] = {
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
          schema.map(GenericData.get().createEnum(symbol, _))
        else
          Left(AvroError.encodeSymbolNotInSchema(symbol, symbols, typeName))
      }, {
        case enum: GenericEnumSymbol[_] =>
          val symbol = enum.toString()

          if (symbols.contains(symbol))
            decode(symbol)
          else
            Left(AvroError.decodeSymbolNotInSchema(symbol, symbols, typeName))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "GenericEnumSymbol", typeName))
      }
    )
  }

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
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A],
    namespace: String,
    aliases: Seq[String] = Seq.empty,
    doc: Option[String] = None,
    props: Props = Props.empty
  ): Codec[A] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val schema = AvroError.catchNonFatal {
      props.toChain.map { props =>
        val schema =
          SchemaBuilder
            .builder()
            .fixed(name)
            .namespace(namespace)
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
      .instance(
        schema,
        a => {
          val bytes = encode(a)
          if (bytes.length <= size) {
            val buffer = ByteBuffer.allocate(size).put(bytes)
            schema.map(GenericData.get().createFixed(null, buffer.array(), _))
          } else {
            Left(AvroError.encodeExceedsFixedSize(bytes.length, size, typeName))
          }
        }, {
          case fixed: GenericFixed =>
            val bytes = fixed.bytes()
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
  implicit final val float: Codec[Float] =
    Codec.instance(
      Right(SchemaBuilder.builder().floatType()),
      java.lang.Float.valueOf(_).asRight, {
        case float: java.lang.Float =>
          Right(float)
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
  final def fromBinary[A](bytes: Array[Byte])(implicit codec: Codec[A]): Either[AvroError, A] =
    codec.schema.flatMap { schema =>
      AvroError.catchNonFatal {
        val bais = new ByteArrayInputStream(bytes)
        val decoder = DecoderFactory.get.binaryDecoder(bais, null)
        val value = new GenericDatumReader[Any](schema).read(null, decoder)
        codec.decode(value)
      }
    }

  /**
    * Returns the result of decoding the specified
    * Avro binary to the specified type.
    *
    * @group Utilities
    */
  final def fromBinary[A](bytes: Array[Byte], writer: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    codec.schema.flatMap { schema =>
      AvroError.catchNonFatal {
        val bais = new ByteArrayInputStream(bytes)
        val decoder = DecoderFactory.get.binaryDecoder(bais, null)
        val value = new GenericDatumReader[Any](schema, writer).read(null, decoder)
        codec.decode(value)
      }
    }

  /**
    * Returns the result of decoding the specified
    * Avro JSON to the specified type.
    *
    * @group Utilities
    */
  final def fromJson[A](json: String)(implicit codec: Codec[A]): Either[AvroError, A] =
    codec.schema.flatMap { schema =>
      AvroError.catchNonFatal {
        val bais = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
        val decoder = DecoderFactory.get.jsonDecoder(schema, bais)
        val value = new GenericDatumReader[Any](schema).read(null, decoder)
        codec.decode(value)
      }
    }

  /**
    * Returns a new [[Codec]] instance using the specified
    * `Schema`, and encode and decode functions.
    *
    * @group Create
    */
  final def instance[A](
    schema: Either[AvroError, Schema],
    encode: A => Either[AvroError, Any],
    decode: Any => Either[AvroError, A]
  ): Codec[A] = {
    val _schema = schema
    val _encode = encode
    val _decode = decode

    new Codec[A] {
      override final val schema: Either[AvroError, Schema] =
        _schema

      override final def encode(a: A): Either[AvroError, Any] =
        _encode(a)

      override final def decode(value: Any): Either[AvroError, A] =
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
  implicit final val instant: Codec[Instant] =
    Codec.instance(
      Right(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())),
      _.asRight, {
        case instant: Instant =>
          Right(instant)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Long", "Instant"))
      }
    )

  /**
    * @group General
    */
  implicit final val int: Codec[Int] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      java.lang.Integer.valueOf(_).asRight, {
        case integer: java.lang.Integer =>
          Right(integer)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Int", "Int"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def list[A](implicit codec: Codec[A]): Codec[List[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList.traverse(codec.decode)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "List"))
      }
    )

  /**
    * @group JavaTime
    */
  implicit final val localDate: Codec[LocalDate] =
    Codec.instance(
      Right(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())),
      localDate => Right(localDate), {
        case localDate: LocalDate =>
          Right(localDate)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "LocalDate", "LocalDate"))
      }
    )

  /**
    * @group General
    */
  implicit final val long: Codec[Long] =
    Codec.instance(
      Right(SchemaBuilder.builder().longType()),
      java.lang.Long.valueOf(_).asRight, {
        case long: java.lang.Long =>
          Right(long)
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Long", "Long"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def map[A](implicit codec: Codec[A]): Codec[Map[String, A]] =
    Codec.instance(
      codec.schema.map(Schema.createMap),
      _.toList
        .traverse {
          case (key, value) =>
            codec
              .encode(value)
              .tupleLeft(new Utf8(key))
        }
        .map(_.toMap.asJava), {
        case map: java.util.Map[_, _] =>
          map.asScala.toList
            .traverse {
              case (key: Utf8, value) =>
                codec.decode(value).tupleLeft(key.toString)
              case (key, _) =>
                Left(AvroError.decodeUnexpectedMapKey(key))
            }
            .map(_.toMap)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "java.util.Map", "Map"))
      }
    )

  /**
    * @group General
    */
  implicit final val none: Codec[None.type] =
    Codec.instance(
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(null),
      value => {
        if (value == null) Right(None)
        else Left(AvroError.decodeUnexpectedType(value, "null", "None"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def nonEmptyChain[A](implicit codec: Codec[A]): Codec[NonEmptyChain[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList
            .traverse(codec.decode)
            .flatMap { list =>
              if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyChain"))
              else Right(NonEmptyChain.fromChainUnsafe(Chain.fromSeq(list)))
            }

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyChain"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def nonEmptyList[A](implicit codec: Codec[A]): Codec[NonEmptyList[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList
            .traverse(codec.decode)
            .flatMap { list =>
              if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyList"))
              else Right(NonEmptyList.fromListUnsafe(list))
            }

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyList"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def nonEmptySet[A](
    implicit codec: Codec[A],
    ordering: Ordering[A]
  ): Codec[NonEmptySet[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList
            .traverse(codec.decode)
            .flatMap { list =>
              if (list.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptySet"))
              else Right(NonEmptySet.fromSetUnsafe(SortedSet(list: _*)))
            }

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptySet"))
      }
    )

  /**
    * @group Cats
    */
  implicit final def nonEmptyVector[A](implicit codec: Codec[A]): Codec[NonEmptyVector[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toVector.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toVector
            .traverse(codec.decode)
            .flatMap { vector =>
              if (vector.isEmpty) Left(AvroError.decodeEmptyCollection("NonEmptyVector"))
              else Right(NonEmptyVector.fromVectorUnsafe(vector))
            }

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "NonEmptyVector"))
      }
    )

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
  )(f: FieldBuilder[A] => FreeApplicative[Field[A, ?], A]): Codec[A] = {
    val typeName = if (namespace.isEmpty) name else s"$namespace.$name"
    val free = f(FieldBuilder.instance)
    val schema = AvroError.catchNonFatal {
      val fields =
        free.analyze {
          λ[Field[A, ?] ~> λ[a => Either[AvroError, Chain[Schema.Field]]]] { field =>
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
    Codec.instance(
      schema,
      a =>
        schema.flatMap { schema =>
          val fields =
            free.analyze {
              λ[Field[A, ?] ~> λ[a => Either[AvroError, Chain[(String, Any)]]]] { field =>
                field.codec.encode(field.access(a)).map(result => Chain.one((field.name, result)))
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
        case record: GenericRecord =>
          free.foldMap {
            λ[Field[A, ?] ~> Either[AvroError, ?]] { field =>
              val schemaField = record.getSchema.getField(field.name)

              if (schemaField != null) {
                field.codec.decode(record.get(field.name))
              } else {
                field.default.toRight(
                  AvroError.decodeMissingRecordField(field.name, typeName)
                )
              }
            }
          }
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
      }
    )
  }

  /**
    * @group Collection
    */
  implicit final def seq[A](implicit codec: Codec[A]): Codec[Seq[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList.traverse(codec.decode)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "Seq"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def set[A](implicit codec: Codec[A]): Codec[Set[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.toList.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toList.traverse(codec.decode).map(_.toSet)

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Collection", "Set"))
      }
    )

  /**
    * @group General
    */
  implicit final val short: Codec[Short] =
    Codec.instance(
      Right(SchemaBuilder.builder().intType()),
      short => Right(java.lang.Integer.valueOf(short.toInt)), {
        case integer: java.lang.Integer =>
          if (Short.MinValue.toInt <= integer && integer <= Short.MaxValue.toInt)
            Right(integer.toShort)
          else Left(AvroError.unexpectedShort(integer))

        case other =>
          Left(AvroError.decodeUnexpectedType(other, "Int", "Short"))
      }
    )

  /**
    * @group General
    */
  implicit final def some[A](implicit codec: Codec[A]): Codec[Some[A]] =
    codec.imap(Some(_))(_.value)

  /**
    * @group General
    */
  implicit final val string: Codec[String] =
    Codec.instance(
      Right(SchemaBuilder.builder().stringType()),
      new Utf8(_).asRight, {
        case string: String =>
          Right(string)
        case utf8: Utf8 =>
          Right(utf8.toString())
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
  final def union[A](f: AltBuilder[A] => Chain[Alt[A]])(
    implicit tag: WeakTypeTag[A]
  ): Codec[A] = {
    val typeName = fullNameFrom(tag)
    val alts = f(AltBuilder.instance)
    val schema = AvroError.catchNonFatal {
      alts.toList
        .traverse(_.codec.schema)
        .map(schemas => Schema.createUnion(schemas.asJava))
    }
    Codec.instance(
      schema,
      a =>
        alts
          .foldMapK { alt =>
            alt.prism.getOption(a).map(alt.codec.encode)
          }
          .getOrElse {
            Left(AvroError.encodeExhaustedAlternatives(a, typeName))
          }, {
        case container: GenericContainer =>
          val altName =
            container.getSchema.getName

          def altMatching =
            alts
              .find(_.codec.schema.exists(_.getName == altName))
              .toRight(AvroError.decodeMissingUnionAlternative(altName, typeName))

          altMatching.flatMap { alt =>
            alt.codec
              .decode(container)
              .map(alt.prism.reverseGet)
          }

        case other =>
          alts
            .collectFirstSome { alt =>
              alt.codec
                .decode(other)
                .map(alt.prism.reverseGet)
                .toOption
            }
            .toRight(AvroError.decodeExhaustedAlternatives(other, typeName))
      }
    )
  }

  /**
    * @group General
    */
  implicit final val unit: Codec[Unit] =
    Codec.instance(
      Right(SchemaBuilder.builder().nullType()),
      _ => Right(null),
      value => {
        if (value == null) Right(())
        else Left(AvroError.decodeUnexpectedType(value, "null", "Unit"))
      }
    )

  /**
    * @group JavaUtil
    */
  implicit final val uuid: Codec[UUID] =
    Codec.instance(
      Right(LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType())),
      uuid => Right(uuid), {
        case uuid: UUID => uuid.asRight
        case other =>
          Left(AvroError.decodeUnexpectedType(other, "UUID", "UUID"))
      }
    )

  /**
    * @group Collection
    */
  implicit final def vector[A](implicit codec: Codec[A]): Codec[Vector[A]] =
    Codec.instance(
      codec.schema.map(Schema.createArray),
      _.traverse(codec.encode).map(_.asJava), {
        case collection: java.util.Collection[_] =>
          collection.asScala.toVector.traverse(codec.decode)

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

  /**
    * @group Create
    */
  sealed abstract class Alt[A] {
    type B

    def codec: Codec[B]

    def prism: Prism[A, B]
  }

  private[vulcan] final object Alt {
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

  private[vulcan] final object AltBuilder {
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

  private[vulcan] final object Field {
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
    )(implicit codec: Codec[B]): FreeApplicative[Field[A, ?], B]
  }

  private[vulcan] final object FieldBuilder {
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
        )(implicit codec: Codec[B]): FreeApplicative[Field[Any, ?], B] =
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
