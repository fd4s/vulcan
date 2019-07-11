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

import cats.data.NonEmptyList
import cats.implicits._
import magnolia._
import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}
import vulcan.internal.converters.collection._

package object generic {
  implicit final val cnilCodec: Codec[CNil] =
    Codec.instance(
      Right(Schema.createUnion()),
      (_, _) => Left(AvroError("CNil")),
      (_, _) => Left(AvroError("Unable to decode to any type in Coproduct"))
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
                      NonEmptyList.of(Schema.Type.RECORD)
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
                  val schemaTypes = schema.getTypes.asScala.toList
                  val subtypes = sealedTrait.subtypes.toList

                  subtypes
                    .zip(schemaTypes)
                    .collectFirstSome {
                      case (subtype, schema) =>
                        val decoded = subtype.typeclass.decode(other, schema)
                        if (decoded.isRight) Some(decoded) else None
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
                    NonEmptyList.of(Schema.Type.UNION)
                  )
              }
          }
        }
      )
    }

    final type Typeclass[A] = Codec[A]
  }
}
