/*
 * Copyright 2019-2021 OVO Energy Limited
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

package object generic {
  implicit final val cnilCodec: Codec.Aux[Nothing, CNil] =
    Codec
      .instance[Nothing, CNil](
        Right(Schema.createUnion()),
        cnil => Left(AvroError.encodeExhaustedAlternatives(cnil, Some("Coproduct"))),
        (value, _) => Left(AvroError.decodeExhaustedAlternatives(value))
      )
      .adaptDecodeError {
        case e @ AvroError.ErrorDecodingType("Coproduct", _) => e
        case other                                           => AvroError.ErrorDecodingType("Coproduct", other)
      }

  implicit final def coproductCodec[H, T <: Coproduct](
    implicit headCodec: Codec[H],
    tailCodec: Lazy[Codec[T]]
  ): Codec[H :+: T] =
    Codec
      .instance[AnyRef, H :+: T](
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
        _.eliminate(
          headCodec.encode(_),
          tailCodec.value.encode(_)
        ),
        (value, schema) => {
          val schemaTypes =
            schema.getType() match {
              case Schema.Type.UNION => schema.getTypes.asScala
              case _                 => Seq(schema)
            }

          value match {
            case container: GenericContainer =>
              headCodec.schema.flatMap { headSchema =>
                val name = container.getSchema.getName
                if (headSchema.getName == name) {
                  val subschema =
                    schemaTypes
                      .find(_.getName == name)
                      .toRight(AvroError.decodeMissingUnionSchema(name))

                  subschema
                    .flatMap(headCodec.decode(container, _))
                    .map(Inl(_))
                } else {
                  tailCodec.value
                    .decode(container, schema)
                    .map(Inr(_))
                }
              }

            case other =>
              headCodec.schema
                .traverse { headSchema =>
                  val headName = headSchema.getName
                  schemaTypes
                    .find(_.getName == headName)
                    .flatMap { schema =>
                      headCodec
                        .decode(other, schema)
                        .map(Inl(_))
                        .toOption
                    }
                }
                .getOrElse {
                  tailCodec.value
                    .decode(other, schema)
                    .map(Inr(_))
                }
          }
        }
      )
      .adaptDecodeError {
        case e @ AvroError.ErrorDecodingType("Coproduct", _) => e
        case other                                           => AvroError.ErrorDecodingType("Coproduct", other)
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

      val typeName =
        s"$namespace.${caseClass.typeName.short}"
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
        }
      Codec
        .instance[AnyRef, A](
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
            (value, schema) => {
              schema.getType() match {
                case Schema.Type.RECORD =>
                  value match {
                    case record: IndexedRecord =>
                      val recordSchema = record.getSchema()
                      val recordFields = recordSchema.getFields()

                      val fields =
                        caseClass.parameters.toList.traverse { param =>
                          val field = recordSchema.getField(param.label)
                          if (field != null) {
                            val value = record.get(recordFields.indexOf(field))
                            param.typeclass.decode(value, field.schema())
                          } else Left(AvroError.decodeMissingRecordField(param.label))
                        }

                      fields.map(caseClass.rawConstruct)

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
        .adaptDecodeError(AvroError.errorDecodingTo(typeName, _))
    }

    /**
      * Returns a `Codec` instance for the specified type,
      * deriving details from the type, as long as the
      * type is a `case class` or `sealed trait`.
      */
    final def derive[A]: Codec[A] =
      macro Magnolia.gen[A]

    final def dispatch[A](sealedTrait: SealedTrait[Codec, A]): Codec.Aux[AnyRef, A] = {
      val typeName = sealedTrait.typeName.full
      Codec
        .instance[AnyRef, A](
          AvroError.catchNonFatal {
            sealedTrait.subtypes.toList
              .traverse(_.typeclass.schema)
              .map(schemas => Schema.createUnion(schemas.asJava))
          },
          a =>
            sealedTrait.dispatch(a) { subtype =>
              subtype.typeclass.encode(subtype.cast(a))
            },
          (value, schema) => {
            val schemaTypes =
              schema.getType() match {
                case Schema.Type.UNION => schema.getTypes.asScala
                case _                 => Seq(schema)
              }

            value match {
              case container: GenericContainer =>
                val subtypeName =
                  container.getSchema.getName

                val subtypeUnionSchema =
                  schemaTypes
                    .find(_.getName == subtypeName)
                    .toRight(AvroError.decodeMissingUnionSchema(subtypeName))

                def subtypeMatching =
                  sealedTrait.subtypes
                    .find(_.typeclass.schema.exists(_.getName == subtypeName))
                    .toRight(AvroError.decodeMissingUnionAlternative(subtypeName))

                subtypeUnionSchema.flatMap { subtypeSchema =>
                  subtypeMatching.flatMap { subtype =>
                    subtype.typeclass.decode(container, subtypeSchema)
                  }
                }

              case other =>
                sealedTrait.subtypes.toList
                  .collectFirstSome { subtype =>
                    subtype.typeclass.schema
                      .traverse { subtypeSchema =>
                        val subtypeName = subtypeSchema.getName
                        schemaTypes
                          .find(_.getName == subtypeName)
                          .flatMap { schema =>
                            subtype.typeclass
                              .decode(other, schema)
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
        .adaptDecodeError(AvroError.errorDecodingTo(typeName, _))
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
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[GenericData.EnumSymbol, A] =
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
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[GenericFixed, A] =
    Codec.fixed(
      name = nameFrom(tag),
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )
}
