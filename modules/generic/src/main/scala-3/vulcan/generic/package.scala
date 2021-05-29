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
    case given ProductInstances[Codec, A] => deriveProduct
    case given CoproductInstances[Codec, A] => deriveCoproduct
    case given ValueOf[A] => sgt
  }

  inline def deriveProduct[A](using inst: ProductInstances[Codec, A]): Derived[Codec.Aux[GenericRecord, A]] = Derived {
    given Mirror.ProductOf[A] = summonInline
    val nullDefaultBase: Boolean = summonFrom {
      case a: Annotation[AvroNullDefault, A] => a.apply().enabled
      case _ => false
    }
    dpImpl(nameOf[A], namespaceOf[A], docOf[A], nullDefaultBase)
  }

  private def dpImpl[A](name: String, namespace: String, doc: Option[String], nullDefaultBase: Boolean)(using nullDefaults: Annotations[AvroNullDefault, A], docs: Annotations[AvroDoc, A], inst: ProductInstances[Codec, A], m: Mirror.ProductOf[A], l: Labelling[A]) = 
    Codec.record[A](name, namespace, doc){ fb => 
      val fields: Chain[Field[A, Any]] = 
      inst.unfold0[(Int, Chain[Field[A, _]])](0, Chain.empty) { [t] => // (inst.instances.toList.asInstanceOf[List[Codec[Any]]]).zipWithIndex.map { 
        (acc: (Int, Chain[Field[A, _]]), codec: Codec[t]) =>
          val i = acc._1
          (i + 1, acc._2.append {
            fb.mk[t](
              name = l.elemLabels(i), 
              access = _.asInstanceOf[Product].productElement(i).asInstanceOf, 
              doc = docs().productElement(i).asInstanceOf[Option[AvroDoc]].map(_.doc),
              default = {
                val fieldNullDefault: Option[Boolean] = nullDefaults().productElement(i).asInstanceOf[Option[AvroNullDefault]].map(_.enabled)
                val wantNullDefault: Boolean = fieldNullDefault.getOrElse(nullDefaultBase)
                if (wantNullDefault && codec.schema.exists(_.isNullable)) codec.schema.flatMap(codec.decode(null, _)).toOption
                else None
              }
            )(using codec)
        })
      }._2.asInstanceOf[Chain[Field[A, Any]]]
      val free: FreeApplicative[Field[A, *], Chain[Any]] = fields.traverse(FreeApplicative.lift)
      free.map{ as => 
          m.fromProduct(Tuple.fromArray(as.toList.toArray))
        }
    }
  
  inline def deriveCoproduct[A](using inline inst: CoproductInstances[Codec, A]): Derived[Codec.Aux[Any, A]] =
    Derived {
      val gen: CoproductGeneric[A] = summonInline
      Codec.union { (alt: AltBuilder[A]) =>
        inst.unfold0(Chain.empty[Alt[A]]) { 
          [t <: A] => 
            (acc: Chain[Alt[A]], codec: Codec[t]) =>
              acc ++ alt[t](using codec, Prism.instance(gen.select[t](_))(identity))
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