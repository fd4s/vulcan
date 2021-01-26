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
import org.apache.avro.Schema
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}
import shapeless.ops.coproduct.{Inject, Selector}
import vulcan.internal.converters.collection._
import vulcan.internal.tags._

package object generic {
  implicit final val cnilCodec: Codec[CNil] =
    Codec.instance(
      Right(Schema.createUnion()),
      cnil => Left(AvroError.encodeExhaustedAlternatives(cnil, Some("Coproduct"))),
      value => Left(AvroError.decodeExhaustedAlternatives(value, Some("Coproduct")))
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
        headCodec.encode(_),
        tailCodec.value.encode(_)
      ),
      value => {

        // value =>
        // alts
        //   .collectFirstSome { alt =>
        //     alt.codec
        //       .decode(value)
        //       .map(alt.prism.reverseGet)
        //       .toOption
        //   }
        //   .toRight {
        //     AvroError.decodeExhaustedAlternatives(value, None)
        //   }
        headCodec
          .decode(value)
          .map(Inl(_))
          .orElse {
            tailCodec.value
              .decode(value)
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
                Avro.Record(values.toMap, schema)
              }
            },
        if (caseClass.isValueClass) { value =>
          caseClass.parameters.head.typeclass
            .decode(value)
            .map(decoded => caseClass.rawConstruct(List(decoded)))
        } else {
          _ match {
            case Avro.Record(fields, _) =>
              val f =
                caseClass.parameters.toList.traverse { param =>
                  fields
                    .get(param.label)
                    .toRight(AvroError.decodeMissingRecordField(param.label, typeName))
                    .flatMap(param.typeclass.decode)
                }

              f.map(caseClass.rawConstruct)

            case other =>
              Left(AvroError.decodeUnexpectedType(other, "IndexedRecord", typeName))
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
        a =>
          sealedTrait.dispatch(a) { subtype =>
            subtype.typeclass.encode(subtype.cast(a))
          },
        value =>
          sealedTrait.subtypes.toList
            .collectFirstSome { subtype =>
              subtype.typeclass.decode(value).toOption
            }
            .toRight(AvroError.decodeExhaustedAlternatives(value, Some(typeName)))
      )
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
  )(implicit tag: WeakTypeTag[A]): Codec[A] =
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
  )(implicit tag: WeakTypeTag[A]): Codec[A] =
    Codec.fixed(
      name = nameFrom(tag),
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )
}
