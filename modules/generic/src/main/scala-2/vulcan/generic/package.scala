/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import scala.language.experimental.macros
import scala.reflect.runtime.universe.WeakTypeTag
import cats.implicits._
import magnolia._
import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}
import shapeless.ops.coproduct.{Inject, Selector}
import vulcan.internal.converters.collection._
import vulcan.internal.tags._
import cats.data.Chain

package object generic {
  implicit final val cnilCodec: Codec.Aux[Nothing, CNil] =
    Codec.UnionCodec(Chain.empty).asInstanceOf[Codec.Aux[Nothing, CNil]].withTypeName("Coproduct")

  implicit final def coproductCodec[H, T <: Coproduct](
    implicit headCodec: Codec[H],
    tailCodec: Lazy[Codec[T]]
  ): Codec[H :+: T] =
    tailCodec.value match {
      case Codec.WithTypeName(u: Codec.UnionCodec[T], typeName) =>
        val tailAlts: Chain[Codec.Alt[H :+: T]] =
          u.alts.map(_.imap[H :+: T](_.eliminate(_ => None, Some(_)), Inr(_)))
        Codec
          .UnionCodec(
            tailAlts
              .prepend(
                Codec.Alt(headCodec, Prism.identity.imap[H :+: T](_.select[H], Inl[H, T](_)))
              )
          )
          .withTypeName(typeName)
      case other =>
        throw new IllegalArgumentException(
          s"cannot derive coproduct codec from non-union ${other.getClass()}"
        )
    }

  implicit final def coproductPrism[C <: Coproduct, A](
    implicit inject: Inject[C, A],
    selector: Selector[C, A]
  ): Prism[C, A] =
    Prism.instance(selector(_))(inject(_))

  implicit final class MagnoliaCodec private[generic] (
    private val codec: Codec.type
  ) extends AnyVal {
    final def combine[A](caseClass: CaseClass[Codec, A]): Codec[A] = {
      val namespace =
        caseClass.annotations
          .collectFirst { case AvroNamespace(namespace) => namespace }
          .getOrElse(caseClass.typeName.owner)

      val shortName =
        caseClass.annotations
          .collectFirst { case AvroName(namespace) => namespace }
          .getOrElse(caseClass.typeName.short)

      val typeName =
        s"$namespace.$shortName"

      val schema =
        if (caseClass.isValueClass) {
          caseClass.parameters.head.typeclass.schema
        } else {
          AvroError.catchNonFatal {
            val nullDefaultBase = caseClass.annotations
              .collectFirst { case AvroNullDefault(enabled) => enabled }
              .getOrElse(false)

            val fields =
              caseClass.parameters.toList.traverse { param =>
                param.typeclass.schema.map { schema =>
                  def nullDefaultField =
                    param.annotations
                      .collectFirst {
                        case AvroNullDefault(nullDefault) => nullDefault
                      }
                      .getOrElse(nullDefaultBase)

                  new Schema.Field(
                    param.label,
                    schema,
                    param.annotations.collectFirst {
                      case AvroDoc(doc) => doc
                    }.orNull,
                    if (schema.isNullable && nullDefaultField) Schema.Field.NULL_DEFAULT_VALUE
                    else null
                  )
                }
              }

            fields.map { fields =>
              Schema.createRecord(
                caseClass.annotations
                  .collectFirst {
                    case AvroName(name) => name
                  }
                  .getOrElse(
                    caseClass.typeName.short
                  ),
                caseClass.annotations.collectFirst {
                  case AvroDoc(doc) => doc
                }.orNull,
                namespace,
                false,
                fields.asJava
              )
            }
          }
        }
      Codec
        .instance[Any, A](
          schema,
          if (caseClass.isValueClass) { a =>
            val param = caseClass.parameters.head
            param.typeclass.encode(param.dereference(a))
          } else
            (a: A) =>
              schema.flatMap { schema =>
                val fields =
                  caseClass.parameters.toList.traverse { param =>
                    param.typeclass
                      .encode(param.dereference(a))
                      .tupleLeft(param.label)
                  }

                fields.map { values =>
                  val record = new GenericData.Record(schema)
                  values.foreach {
                    case (label, value) =>
                      record.put(label, value)
                  }

                  record
                }
              },
          if (caseClass.isValueClass) { (value, schema) =>
            caseClass.parameters.head.typeclass
              .decode(value, schema)
              .map(decoded => caseClass.rawConstruct(List(decoded)))
          } else
            (value, writerSchema) => {
              writerSchema.getType() match {
                case Schema.Type.RECORD =>
                  value match {
                    case record: IndexedRecord =>
                      caseClass.parameters.toList
                        .traverse {
                          param =>
                            val field = record.getSchema.getField(param.label)
                            if (field != null) {
                              val value = record.get(field.pos)
                              param.typeclass.decode(value, field.schema())
                            } else {
                              schema.flatMap { readerSchema =>
                                readerSchema.getFields.asScala
                                  .find(_.name == param.label)
                                  .filter(_.hasDefaultValue)
                                  .toRight(AvroError.decodeMissingRecordField(param.label))
                                  .flatMap(
                                    readerField => param.typeclass.decode(null, readerField.schema)
                                  )
                              }
                            }
                        }
                        .map(caseClass.rawConstruct)

                    case other =>
                      Left(AvroError.decodeUnexpectedType(other, "IndexedRecord"))
                  }

                case schemaType =>
                  Left {
                    AvroError
                      .decodeUnexpectedSchemaType(
                        schemaType,
                        Schema.Type.RECORD
                      )
                  }
              }
            }
        )
        .withTypeName(typeName)
    }

    /**
      * Returns a `Codec` instance for the specified type,
      * deriving details from the type, as long as the
      * type is a `case class` or `sealed trait`.
      */
    final def derive[A]: Codec[A] =
      macro Magnolia.gen[A]

    final def dispatch[A](sealedTrait: SealedTrait[Codec, A]): Codec.Aux[Any, A] = {

      Codec
        .union[A](
          alt =>
            Chain.fromSeq(sealedTrait.subtypes).flatMap { subtype =>
              implicit val codec: Codec[subtype.SType] = subtype.typeclass
              implicit val prism: Prism[A, subtype.SType] =
                Prism.identity.imap(subtype.cast.lift, identity)
              alt[subtype.SType]
            }
        )
        .changeTypeName(sealedTrait.typeName.full)

    }

    final type Typeclass[A] = Codec[A]
  }

  /**
    * Returns an enum `Codec` for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using type tags.
    *
    * @group Derive
    */
  final def deriveEnum[A](
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[Avro.EnumSymbol, A] =
    Codec.enumeration(
      name = nameFrom(tag),
      symbols = symbols,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )

  /**
    * Returns a fixed `Codec` for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using type tags.
    *
    * @group Derive
    */
  final def deriveFixed[A](
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[Avro.Fixed, A] =
    Codec.fixed(
      name = nameFrom(tag),
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )
}
