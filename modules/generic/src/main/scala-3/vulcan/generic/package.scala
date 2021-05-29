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
import shapeless3.deriving.K0._
import scala.compiletime._
import scala.deriving.Mirror
import scala.reflect.ClassTag
import cats.data.Chain
import cats.free.FreeApplicative

final case class Derived[+A](value: A)

package object generic extends generic.LowPriority {
  implicit final class CodecOps private[generic] (
    private val codec: Codec.type
  ) extends AnyVal {
    transparent inline def derive[A](using inst: Instances[Codec, A]) = generic.derive[A].value
  }

  transparent inline given derive[A](using inst: Instances[Codec, A]): Derived[Codec[A]] = inline inst match {
    case i: ProductInstances[Codec, A] => deriveProduct[A](using i, summonInline)
    case i: CoproductInstances[Codec, A] => deriveCoproduct[A](using i)
    case v: ValueOf[A] => sgt[A](using v)
  }

  inline def deriveProduct[A](using inst: ProductInstances[Codec, A], m: Mirror.ProductOf[A]): Derived[Codec.Aux[GenericRecord, A]] = Derived {
    val l = summonInline[Labelling[A]]
    val nullDefaultBase: Boolean = summonFrom {
      case a: Annotation[AvroNullDefault, A] => a.apply().enabled
      case _ => false
    }
    val nullDefaults = Annotations[AvroNullDefault, A].apply()
    val docs = Annotations[AvroDoc, A].apply()
    Codec.record("as", namespaceOf[A], docOf[A]){
      fb => 
        
        val fields: List[Field[A, Any]] = (inst.instances.toList.asInstanceOf[List[Codec[Any]]]).zipWithIndex.map { 
          (codec, i) => 
            fb.mk(
              name = l.elemLabels(i), 
              access = _.asInstanceOf[Product].productElement(i), 
              doc = docs.productElement(i).asInstanceOf[Option[AvroDoc]].map(_.doc),
              default = {
                val fieldNullDefault: Option[Boolean] = nullDefaults.productElement(i).asInstanceOf[Option[AvroNullDefault]].map(_.enabled)
                val wantNullDefault: Boolean = fieldNullDefault.getOrElse(nullDefaultBase)
                if (wantNullDefault && codec.schema.exists(_.isNullable)) codec.schema.flatMap(codec.decode(null, _)).toOption
                else None
              }
            )(using codec)
        }
        val free = fields.traverse(FreeApplicative.lift)
        
        free.map(as => m.fromProduct(Tuple.fromArray(as.toArray)))
      }
    }

  private def dpImpl[A](using Annotations[AvroNullDefault, A], Annotations[AvroDoc, A])(using inst: ProductInstances[Codec, A], m: Mirror.ProductOf[A], l: Labelling[A]) = ???
  
  inline def deriveCoproduct[A](using inline inst: CoproductInstances[Codec, A]): Derived[Codec.Aux[Any, A]] =
    Derived {
      val m = summonInline[Mirror.SumOf[A]]
      Codec.union { (alt: AltBuilder[A]) =>
        Chain.fromSeq(inst.instances.toList.asInstanceOf[List[Codec[Any]]].zipWithIndex).flatMap { 
          (codec, i) => 
              val prism: Prism[A, A] = Prism.instance[A, A](a => if (m.ordinal(a) == i) Some(a) else None)(identity)
              alt[A](using codec.asInstanceOf, prism)
          }    
      }    
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

}

package generic {
  trait LowPriority {

    inline given fromDerived[A](using derived: Derived[Codec[A]]): Codec[A] = derived.value

    protected inline def sgt[A](using ValueOf[A]): Derived[Codec[A]] =
        Derived(Codec.record(nameOf[A], namespaceOf[A], docOf[A])(_ => FreeApplicative.pure(valueOf[A])))
        
    protected inline def nameOf[A]: String =
      summonFrom {
        case ct: ClassTag[A] => ct.runtimeClass.getSimpleName.stripSuffix("$")
      }

    protected inline def namespaceOf[A]: String =
      summonFrom {
        case a: Annotation[AvroNamespace, A] => a().namespace
        case ct: ClassTag[A] => ct.runtimeClass.getPackage.getName
        case _ => ""
      }

    protected inline def docOf[A]: Option[String] =
      summonFrom {
        case a: Annotation[AvroDoc, A] => Some(a().doc)
        case _ => None
      }
  }
}