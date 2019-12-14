/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.implicits._
import magnolia._
import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}
import shapeless.ops.coproduct.{Inject, Selector}
import vulcan.internal.converters.collection._

package object generic {
  implicit final val cnilCodec: Codec[CNil] =
    Codec.instance(
      Right(Schema.createUnion()),
      cnil => Left(AvroError.encodeExhaustedAlternatives(cnil, "Coproduct")),
      (value, _) => Left(AvroError.decodeExhaustedAlternatives(value, "Coproduct"))
    )

  implicit final def coproductCodec[H, T <: Coproduct](
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
      _.eliminate(
        headCodec.encode,
        tailCodec.value.encode
      ),
      (value, schema) => {
        schema.getType() match {
          case Schema.Type.UNION =>
            value match {
              case container: GenericContainer =>
                headCodec.schema.flatMap {
                  headSchema =>
                    val name = container.getSchema.getFullName
                    if (headSchema.getFullName == name) {
                      val subschema =
                        schema.getTypes.asScala
                          .find(_.getFullName == name)
                          .toRight(AvroError.decodeMissingUnionSchema(name, "Coproduct"))

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
                val schemaTypes =
                  schema.getTypes.asScala

                headCodec.schema
                  .traverse { headSchema =>
                    val headName = headSchema.getFullName
                    schemaTypes
                      .find(_.getFullName == headName)
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

          case schemaType =>
            Left {
              AvroError
                .decodeUnexpectedSchemaType(
                  "Coproduct",
                  schemaType,
                  Schema.Type.UNION
                )
            }
        }
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
                      Schema.Type.RECORD
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
                      Left(
                        AvroError.decodeUnexpectedRecordName(recordSchema.getFullName(), typeName)
                      )

                  case other =>
                    Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
                }

              case schemaType =>
                Left {
                  AvroError
                    .decodeUnexpectedSchemaType(
                      typeName,
                      schemaType,
                      Schema.Type.RECORD
                    )
                }
            }
          }
        }
      )
    }

    /**
      * Returns a `Codec` instance for the specified type,
      * deriving details from the type, as long as the
      * type is a `case class` or `sealed trait`.
      */
    final def derive[A]: Codec[A] =
      macro Magnolia.gen[A]

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
                  subtype.typeclass.schema.flatMap { subtypeSchema =>
                    val subtypeName =
                      subtypeSchema.getFullName

                    val subtypeUnionSchema =
                      schema.getTypes.asScala
                        .find(_.getFullName == subtypeName)
                        .toRight(AvroError.encodeMissingUnionSchema(subtypeName, typeName))

                    subtypeUnionSchema.flatMap(subtype.typeclass.encode(subtype.cast(a), _))
                  }
              }

            case schemaType =>
              Left {
                AvroError
                  .encodeUnexpectedSchemaType(
                    typeName,
                    schemaType,
                    Schema.Type.UNION
                  )
              }
          }
        },
        (value, schema) => {
          schema.getType() match {
            case Schema.Type.UNION =>
              value match {
                case container: GenericContainer =>
                  val subtypeName =
                    container.getSchema.getFullName

                  val subtypeUnionSchema =
                    schema.getTypes.asScala
                      .find(_.getFullName == subtypeName)
                      .toRight(AvroError.decodeMissingUnionSchema(subtypeName, typeName))

                  def subtypeMatching =
                    sealedTrait.subtypes
                      .find(_.typeclass.schema.exists(_.getFullName == subtypeName))
                      .toRight(AvroError.decodeMissingUnionAlternative(subtypeName, typeName))

                  subtypeUnionSchema.flatMap { subtypeSchema =>
                    subtypeMatching.flatMap { subtype =>
                      subtype.typeclass.decode(container, subtypeSchema)
                    }
                  }

                case other =>
                  val schemaTypes =
                    schema.getTypes.asScala

                  sealedTrait.subtypes.toList
                    .collectFirstSome { subtype =>
                      subtype.typeclass.schema
                        .traverse { subtypeSchema =>
                          val subtypeName = subtypeSchema.getFullName
                          schemaTypes
                            .find(_.getFullName == subtypeName)
                            .flatMap { schema =>
                              subtype.typeclass
                                .decode(other, schema)
                                .toOption
                            }
                        }
                    }
                    .getOrElse {
                      Left(AvroError.decodeExhaustedAlternatives(other, typeName))
                    }
              }

            case schemaType =>
              Left {
                AvroError
                  .decodeUnexpectedSchemaType(
                    typeName,
                    schemaType,
                    Schema.Type.UNION
                  )
              }
          }
        }
      )
    }

    final type Typeclass[A] = Codec[A]
  }
}
