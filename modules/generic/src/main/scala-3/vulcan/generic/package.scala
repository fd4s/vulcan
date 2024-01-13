/*
 * Copyright 2019-2024 OVO Energy Limited
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
import cats.data.Chain
import cats.implicits._
import cats.free.FreeApplicative
import magnolia1._
import org.apache.avro.generic._
import org.apache.avro.Schema
import vulcan.internal.converters.collection._

package object generic {

  implicit final class MagnoliaCodec private[generic] (
    private val codec: Codec.type
  ) extends Derivation[Codec] {
    inline def derive[A](using Mirror.Of[A]): Codec[A] = derived[A]

    final def join[A](caseClass: CaseClass[Codec, A]): Codec[A] = 
      Codec
          .record[A](
            name = caseClass.annotations
              .collectFirst { case AvroName(namespace) => namespace }
              .getOrElse(caseClass.typeInfo.short),
            namespace = caseClass.annotations
              .collectFirst { case AvroNamespace(namespace) => namespace }
              .getOrElse(caseClass.typeInfo.owner),
            doc = caseClass.annotations.collectFirst {
              case AvroDoc(doc) => doc
            }
          ) { (f: Codec.FieldBuilder[A]) =>
            val nullDefaultBase = caseClass.annotations
              .collectFirst { case AvroNullDefault(enabled) => enabled }
              .getOrElse(false)

            caseClass.params.toList
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
                  access = param.deref,
                  doc = param.annotations.collectFirst {
                    case AvroDoc(doc) => doc
                  },
                  default = param.default.orElse(if (codec.schema.exists(_.isNullable) && nullDefaultField) Some(None.asInstanceOf[param.PType])  // TODO: remove cast
                             else None)
                ).widen
              }
              .map(caseClass.rawConstruct(_))
          }

    final def split[A](sealedTrait: SealedTrait[Codec, A]): Codec.Aux[Any, A] = {
      Codec
        .union[A](
          alt =>
            Chain.fromSeq(sealedTrait.subtypes.sortBy(_.typeInfo.full))
              .flatMap { subtype =>
                alt(subtype.typeclass, Prism.instance(subtype.cast.lift)(identity))
              }
        )
        .withTypeName(sealedTrait.typeInfo.full)
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
    case a: Annotation[AvroName, A] => a().name
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
