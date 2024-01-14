/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import scala.language.experimental.macros
import scala.reflect.runtime.universe.WeakTypeTag
import cats.implicits._
import magnolia._
import shapeless.{:+:, CNil, Coproduct, Inl, Inr, Lazy}
import shapeless.ops.coproduct.{Inject, Selector}
import vulcan.internal.tags._
import cats.data.Chain
import cats.free.FreeApplicative

package object generic {
  implicit final val cnilCodec: Codec.Aux[Nothing, CNil] =
    Codec.UnionCodec(Chain.empty).asInstanceOf[Codec.Aux[Nothing, CNil]].withTypeName("Coproduct")

  implicit final def coproductCodec[H, T <: Coproduct](
    implicit headCodec: Codec[H],
    tailCodec: Lazy[Codec[T]]
  ): Codec[H :+: T] =
    tailCodec.value match {
      case Codec.WithTypeName(Codec.Validated(u: Codec.UnionCodec[T], _), typeName) =>
        val tailAlts: Chain[Codec.Alt[H :+: T]] =
          u.alts.map(_.imap[H :+: T](_.eliminate(_ => None, Some(_)), Inr(_)))
        Codec
          .UnionCodec(
            tailAlts
              .prepend(
                Codec.Alt(headCodec, Prism.instance[H :+: T, H](_.select)(Inl(_)))
              )
          )
          .withTypeName(typeName)
      case Codec.Fail(error) => Codec.Fail(error)
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
    final def combine[A](caseClass: CaseClass[Codec, A]): Codec[A] =
      if (caseClass.isValueClass) {
        val param = caseClass.parameters.head
        param.typeclass.imap(value => caseClass.rawConstruct(List(value)))(param.dereference)
      } else {

        Codec
          .record[A](
            name = caseClass.annotations
              .collectFirst { case AvroName(namespace) => namespace }
              .getOrElse(caseClass.typeName.short),
            namespace = caseClass.annotations
              .collectFirst { case AvroNamespace(namespace) => namespace }
              .getOrElse(caseClass.typeName.owner),
            doc = caseClass.annotations.collectFirst {
              case AvroDoc(doc) => doc
            }
          ) { (f: Codec.FieldBuilder[A]) =>
            val nullDefaultBase = caseClass.annotations
              .collectFirst { case AvroNullDefault(enabled) => enabled }
              .getOrElse(false)

            caseClass.parameters.toList
              .traverse[FreeApplicative[Codec.Field[A, *], *], Any] { param =>
                def nullDefaultField =
                  param.annotations
                    .collectFirst {
                      case AvroNullDefault(nullDefault) => nullDefault
                    }
                    .getOrElse(nullDefaultBase)

                def renamedField =
                  param.annotations
                    .collectFirst {
                      case AvroName(newName) => newName
                    }

                implicit val codec = param.typeclass

                f(
                  name = renamedField.getOrElse(param.label),
                  access = param.dereference,
                  doc = param.annotations.collectFirst {
                    case AvroDoc(doc) => doc
                  },
                  default = param.default.orElse(
                    if (codec.schema.exists(_.isNullable) && nullDefaultField)
                      Some(None.asInstanceOf[param.PType]) // TODO: remove cast
                    else None
                  )
                ).widen
              }
              .map(caseClass.rawConstruct(_))
          }
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
              alt(subtype.typeclass, Prism.instance(subtype.cast.lift)(identity))
            }
        )
        .withTypeName(sealedTrait.typeName.full)
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
