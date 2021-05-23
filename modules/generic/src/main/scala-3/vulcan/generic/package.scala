/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.syntax.all._
import vulcan.internal.converters.collection._
import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless3.deriving._
import scala.compiletime._
import scala.deriving.Mirror
import scala.reflect.ClassTag
import cats.data.Chain

package object generic {

  inline def deriveCoproduct[A](using m: Mirror.SumOf[A]): Codec[A] =
    Codec.union(_ => Chain.fromSeq(summonAlts[A]))

  private inline def summonAll[T <: Tuple]: List[Codec[_]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts) => summonInline[Codec[t]] :: summonAll[ts]

  private inline def derivePrisms[A, T <: Tuple](i: Int)(using m: Mirror.SumOf[A]): List[Prism[A, A]] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts) => Prism.instance[A, A](a => if (m.ordinal(a) == i) Some(a) else None)(identity) :: derivePrisms[A, ts](i + 1)
    }

  private inline def summonAlts[A](using m: Mirror.SumOf[A]): List[Codec.Alt[A]] =
    summonAll[m.MirroredElemTypes].zip(derivePrisms[A, m.MirroredElemTypes](0)).map { 
      (codec, prism) => Codec.Alt[A, A](codec.asInstanceOf, prism)
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
  ): Codec.Aux[GenericData.EnumSymbol, A] =
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
  ): Codec.Aux[GenericFixed, A] =
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
