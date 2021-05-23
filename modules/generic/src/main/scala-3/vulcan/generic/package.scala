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
import cats.free.FreeApplicative

package object generic {
  transparent inline def derive[A](using m: Mirror.Of[A]) = inline m match {
    case mp: Mirror.ProductOf[A] => deriveProduct[A](using mp)
    case ms: Mirror.SumOf[A] => deriveCoproduct[A](using ms)
  }

  inline def deriveProduct[A](using m: Mirror.ProductOf[A]): Codec.Aux[GenericRecord, A] = 
    Codec.record(nameOf[A], namespaceOf[A], docOf[A])(_ => deriveFields[A].map(fields => m.fromProduct(toTuple(fields))))

  inline def deriveCoproduct[A](using Mirror.SumOf[A]): Codec.Aux[Any, A] =
    Codec.union(_ => Chain.fromSeq(summonAlts[A]))

  private inline def summonAlts[A](using m: Mirror.SumOf[A]): List[Codec.Alt[A]] =
    summonAll[m.MirroredElemTypes].zip(derivePrisms[A, m.MirroredElemTypes](0)).map { 
      (codec, prism) => Codec.Alt[A, A](codec.asInstanceOf, prism)
    }

  private inline def summonAll[T <: Tuple]: List[Codec[_]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts) => summonInline[Codec[t]] :: summonAll[ts]

  private inline def derivePrisms[A, T <: Tuple](i: Int)(using m: Mirror.SumOf[A]): List[Prism[A, A]] =
    inline erasedValue[T] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts) => Prism.instance[A, A](a => if (m.ordinal(a) == i) Some(a) else None)(identity) :: derivePrisms[A, ts](i + 1)
    }

  private inline def valueAt[A](n: Int): Any = erasedValue[A] match {
    case _: (a *: as) =>
      n match {
        case 0 => constValue[a]
        case _ => valueAt[as](n-1)
      }
  }

  private inline def deriveFields[A](using m: Mirror.ProductOf[A]): FreeApplicative[Codec.Field[A, *], List[_]] =
    val l = Labelling[A]
    summonAll[m.MirroredElemTypes].zipWithIndex.traverse { 
      case (codec, i) => Codec.FieldBuilder.instance[A].apply[Any](l.elemLabels(i), _.asInstanceOf[Product].productElement(i))(using codec.asInstanceOf[Codec[Any]])
    }

  private def toTuple(l: List[_]): Tuple = l match {
    case Nil => EmptyTuple
    case t :: ts => t *: toTuple(ts)
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
