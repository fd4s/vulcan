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
      value => Left(AvroError.decodeExhaustedAlternatives(value, "Coproduct"))
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
      ), {
        case container: GenericContainer =>
          headCodec.schema.flatMap { headSchema =>
            val name = container.getSchema.getName
            if (headSchema.getName == name) {
              headCodec
                .decode(container)
                .map(Inl(_))
            } else {
              tailCodec.value
                .decode(container)
                .map(Inr(_))
            }
          }

        case other =>
          headCodec
            .decode(other)
            .map(Inl(_))
            .orElse {
              tailCodec.value
                .decode(other)
                .map(Inr(_))
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
      val schema =
        if (caseClass.isValueClass) {
          caseClass.parameters.head.typeclass.schema
        } else {
          AvroError.catchNonFatal {
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
        }
      Codec.instance(
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
        if (caseClass.isValueClass) {
          caseClass.parameters.head.typeclass
            .decode(_)
            .map(decoded => caseClass.rawConstruct(List(decoded)))
        } else {
          case record: GenericRecord =>
            caseClass.parameters.toList
              .traverse { param =>
                val field = record.getSchema.getField(param.label)
                if (field != null) {
                  val value = record.get(param.label)
                  param.typeclass.decode(value)
                } else Left(AvroError.decodeMissingRecordField(param.label, typeName))
              }
              .map(caseClass.rawConstruct)

          case other =>
            Left(AvroError.decodeUnexpectedType(other, "GenericRecord", typeName))
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
        a =>
          sealedTrait.dispatch(a) { subtype =>
            subtype.typeclass.encode(subtype.cast(a))
          }, {
          case container: GenericContainer =>
            val subtypeName =
              container.getSchema.getName

            def subtypeMatching =
              sealedTrait.subtypes
                .find(_.typeclass.schema.exists(_.getName == subtypeName))
                .toRight(AvroError.decodeMissingUnionAlternative(subtypeName, typeName))

            subtypeMatching.flatMap { subtype =>
              subtype.typeclass.decode(container)
            }

          case other =>
            sealedTrait.subtypes.toList
              .collectFirstSome { subtype =>
                subtype.typeclass
                  .decode(other)
                  .toOption
              }
              .toRight {
                AvroError.decodeExhaustedAlternatives(other, typeName)
              }
        }
      )
    }

    final type Typeclass[A] = Codec[A]
  }
}
