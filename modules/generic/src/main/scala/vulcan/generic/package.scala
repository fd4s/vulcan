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

package object generic {
  import vulcan.generic.internal.tags._

  implicit final val cnilCodec: Codec.Aux[Nothing, CNil] =
    Codec
      .instance[Nothing, CNil](
        Schema.createUnion(),
        cnil => Left(AvroError.encodeExhaustedAlternatives(cnil)),
        (value, _) => Left(AvroError.decodeExhaustedAlternatives(value))
      )
      .withTypeName("Coproduct")

  implicit final def coproductCodec[H, T <: Coproduct](
    implicit headCodec: Codec[H],
    tailCodec: Lazy[Codec[T]]
  ): Codec[H :+: T] =
    Codec
      .instance[AnyRef, H :+: T](
        AvroError
          .catchNonFatal {
            val first = headCodec.schema
            val rest = tailCodec.value.schema
            rest.getType() match {
              case Schema.Type.UNION =>
                val schemas = first :: rest.getTypes().asScala.toList
                Right(Schema.createUnion(schemas.asJava))
              case schemaType =>
                Left(AvroError(s"Unexpected schema type $schemaType in Coproduct"))
            }
          }
          .leftMap[Schema](err => throw err.throwable)
          .merge,
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
            case container: GenericContainer => {
              val headSchema = headCodec.schema
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
              schemaTypes
                .find(_.getName == headCodec.schema.getName)
                .flatMap { schema =>
                  headCodec
                    .decode(other, schema)
                    .map(Inl(_))
                    .toOption
                }
                .map(Right(_))
                .getOrElse {
                  tailCodec.value
                    .decode(other, schema)
                    .map(Inr(_))
                }
          }
        }.leftMap {
          case e @ AvroError.ErrorDecodingType("Coproduct", _) => e
          case other                                           => AvroError.ErrorDecodingType("Coproduct", other)
        }
      )

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
      val schema = AvroError
        .catchNonFatal {
          Right {
            if (caseClass.isValueClass) {
              caseClass.parameters.head.typeclass.schema
            } else {
              val nullDefaultBase = caseClass.annotations
                .collectFirst { case AvroNullDefault(enabled) => enabled }
                .getOrElse(false)

              val fields =
                caseClass.parameters.toList.map { param =>
                  val fieldSchema = param.typeclass.schema
                  def nullDefaultField =
                    param.annotations
                      .collectFirst {
                        case AvroNullDefault(nullDefault) => nullDefault
                      }
                      .getOrElse(nullDefaultBase)
                  new Schema.Field(
                    param.label,
                    fieldSchema,
                    param.annotations.collectFirst {
                      case AvroDoc(doc) => doc
                    }.orNull,
                    if (fieldSchema.isNullable && nullDefaultField) Schema.Field.NULL_DEFAULT_VALUE
                    else null
                  )
                }

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
        .leftMap[Schema](err => throw err.throwable)
        .merge
      Codec
        .instance[AnyRef, A](
          schema,
          if (caseClass.isValueClass) { a =>
            val param = caseClass.parameters.head
            param.typeclass.encode(param.dereference(a))
          } else
            (a: A) => {
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
        .withTypeName(typeName)
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
          AvroError
            .catchNonFatal(
              Right(
                Schema.createUnion(
                  sealedTrait.subtypes.toList
                    .map(_.typeclass.schema)
                    .asJava
                )
              )
            )
            .leftMap(err => throw err.throwable)
            .merge,
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
                    .find(_.typeclass.schema.getName == subtypeName)
                    .toRight(AvroError.decodeMissingUnionAlternative(subtypeName))

                subtypeUnionSchema.flatMap { subtypeSchema =>
                  subtypeMatching.flatMap { subtype =>
                    subtype.typeclass.decode(container, subtypeSchema)
                  }
                }

              case other =>
                sealedTrait.subtypes.toList
                  .collectFirstSome { subtype =>
                    Right(subtype.typeclass.schema)
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
        .withTypeName(typeName)
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
