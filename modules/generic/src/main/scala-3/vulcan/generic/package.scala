/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan


import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless3.deriving._
import scala.compiletime._
import scala.reflect.ClassTag
import scala.deriving.Mirror
import cats.implicits._
import magnolia1._
import org.apache.avro.generic._
import org.apache.avro.Schema
import vulcan.internal.converters.collection._

package object generic {

  implicit final class MagnoliaCodec private[generic] (
    private val codec: Codec.type
  ) extends Derivation[Codec] {
    inline def derive[A](using Mirror.Of[A]): Codec[A] = derived[A]

    final def join[A](caseClass: CaseClass[Codec, A]): Codec[A] = {
      val namespace =
        caseClass.annotations
          .collectFirst { case AvroNamespace(namespace) => namespace }
          .getOrElse(caseClass.typeInfo.owner)

      val typeName =
        s"$namespace.${caseClass.typeInfo.short}"
      val schema =
        if (caseClass.isValueClass) {
          caseClass.params.head.typeclass.schema
        } else {
          AvroError.catchNonFatal {
            val nullDefaultBase = caseClass.annotations
              .collectFirst { case AvroNullDefault(enabled) => enabled }
              .getOrElse(false)

            val fields =
              caseClass.params.toList.traverse { param =>
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
                caseClass.typeInfo.short,
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
            val param = caseClass.params.head
            param.typeclass.encode(param.deref(a))
          } else
            (a: A) =>
              schema.flatMap { schema =>
                val fields =
                  caseClass.params.toList.traverse { param =>
                    param.typeclass
                      .encode(param.deref(a))
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
            caseClass.params.head.typeclass
              .decode(value, schema)
              .map(decoded => caseClass.rawConstruct(List(decoded)))
          } else
            (value, writerSchema) => {
              writerSchema.getType() match {
                case Schema.Type.RECORD =>
                  value match {
                    case record: IndexedRecord =>
                      caseClass.params.toList
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

    final def split[A](sealedTrait: SealedTrait[Codec, A]): Codec.Aux[Any, A] = {
      val typeName = sealedTrait.typeInfo.full
      Codec
        .instance[Any, A](
          AvroError.catchNonFatal {
            sealedTrait.subtypes.toList
              .sortBy(_.typeInfo.full)
              .traverse(_.typeclass.schema)
              .map(schemas => Schema.createUnion(schemas.asJava))
          },
          a =>
            sealedTrait.choose(a) { subtype =>
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
        .withTypeName(typeName)
    }

    final type Typeclass[A] = Codec[A]
  }


  /**
    * Returns an enum `Codec` for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using reflection.
    *
    * @group Derive
    */
  inline def deriveEnum[A](
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A]
  ): Codec.Aux[Avro.EnumSymbol, A] =
    Codec.enumeration(
      name = nameOf[A],
      symbols = symbols,
      encode = encode,
      decode = decode,
      namespace = namespaceOf[A],
      doc = docOf[A]
    )

  /**
    * Returns a fixed `Codec` for type `A`, deriving details
    * like the name, namespace, and [[AvroDoc]] documentation
    * from the type `A` using reflection.
    *
    * @group Derive
    */
  inline def deriveFixed[A](
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A]
  ): Codec.Aux[Avro.Fixed, A] =
    Codec.fixed(
      name = nameOf[A],
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceOf[A],
      doc = docOf[A]
    )


  private inline def nameOf[A]: String = summonFrom {
    case ct: ClassTag[A] => ct.runtimeClass.getSimpleName
  }

  private inline def namespaceOf[A]: String = summonFrom {
    case a: Annotation[AvroNamespace, A] => a().namespace
    case ct: ClassTag[A] => ct.runtimeClass.getPackage.getName
  }

  private inline def docOf[A]: Option[String] = summonFrom {
    case a: Annotation[AvroDoc, A] => Some(a().doc)
    case _ => None
  }
}
