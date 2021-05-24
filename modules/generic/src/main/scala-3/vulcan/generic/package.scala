/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.syntax.all._
import vulcan.internal.converters.collection._
import vulcan.Codec.{Alt, AltBuilder, Field}
import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless3.deriving._
import scala.compiletime._
import scala.deriving.Mirror
import scala.reflect.ClassTag
import cats.data.Chain
import cats.free.FreeApplicative

package object generic {
  implicit final class CodecOps private[generic] (
    private val codec: Codec.type
  ) extends AnyVal {
    transparent inline def derive[A](using m: Mirror.Of[A]) = generic.derive[A]
  }

  transparent inline def derive[A](using m: Mirror.Of[A]) = inline m match {
    case mp: Mirror.ProductOf[A] => deriveProduct[A](using mp)
    case ms: Mirror.SumOf[A] => deriveCoproduct[A](using ms)
  }

  inline def deriveProduct[A](using m: Mirror.ProductOf[A]): Codec.Aux[GenericRecord, A] = 
    Codec.record(nameOf[A], namespaceOf[A], docOf[A])(_ => deriveFields[A].map(fields => m.fromProduct(Tuple.fromArray(fields.toArray))))

  private inline def deriveFields[A](using m: Mirror.ProductOf[A]): FreeApplicative[Codec.Field[A, *], List[_]] =
    val l = Labelling[A]
    val nullDefaultBase: Boolean = summonFrom {
      case a: Annotation[AvroNullDefault, A] => a.apply().enabled
      case _ => false
    }
    val nullDefaults = Annotations[AvroNullDefault, A].apply()
    val docs = Annotations[AvroDoc, A].apply()
    summonAllCodecs[m.MirroredElemTypes].zipWithIndex.traverse { 
      case (codec, i) => 
        Codec.FieldBuilder.instance[A].apply[Any](
          name = l.elemLabels(i), 
          access = _.asInstanceOf[Product].productElement(i), 
          doc = docs.productElement(i).asInstanceOf[Option[AvroDoc]].map(_.doc),
          default = {
            val fieldNullDefault: Option[Boolean] = nullDefaults.productElement(i).asInstanceOf[Option[AvroNullDefault]].map(_.enabled)
            val wantNullDefault: Boolean = fieldNullDefault.getOrElse(nullDefaultBase)
            if (wantNullDefault && codec.schema.exists(_.isNullable)) codec.schema.flatMap(codec.decode(null, _)).toOption
            else None
          }
        )(using codec.asInstanceOf[Codec[Any]])
    }
  
  inline def deriveCoproduct[A](using m: Mirror.SumOf[A]): Codec.Aux[Any, A] =
    Codec.union { (alt: AltBuilder[A]) =>
      Chain.fromSeq(summonAllCodecs[m.MirroredElemTypes].zipWithIndex).flatMap { 
        (codec, i) => 
          val prism: Prism[A, A] = Prism.instance[A, A](a => if (m.ordinal(a) == i) Some(a) else None)(identity)
          alt[A](using codec.asInstanceOf, prism)
      }    
    }

  private inline def summonAllCodecs[T <: Tuple]: List[Codec[_]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts) => summonOrDerive[t] :: summonAllCodecs[ts]

  private inline def summonOrDerive[A]: Codec[A] = summonFrom {
    case c: Codec[A] => c
    case m: Mirror.Of[A] => derive[A](using m)
    case ct: ClassTag[A] =>
      given ct0: ClassTag[A] = ct
      Codec.record(nameOf[A], namespaceOf[A], docOf[A])(_ => FreeApplicative.pure(summonInline[ValueOf[A]].value))
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
    case ct: ClassTag[A] => ct.runtimeClass.getSimpleName.stripSuffix("$")
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
