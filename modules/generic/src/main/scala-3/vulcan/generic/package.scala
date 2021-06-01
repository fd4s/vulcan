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

package object generic extends vulcan.generic.LowPriority {
  implicit final class CodecOps private[generic] (
    private val codec: Codec.type
  ) extends AnyVal {
    transparent inline def derive[A](using inst: Instances[Codec, A]) = vulcan.generic.derive[A].value
  }

  transparent inline given derive[A](using inst: Instances[Codec, A]): Derived[Codec[A]] = inline inst match {
    case given ProductInstances[Codec, A] => deriveProduct
    case given CoproductInstances[Codec, A] => deriveCoproduct
    case given ValueOf[A] => sgt
  }

  inline def deriveProduct[A](using inst: ProductInstances[Codec, A]): Derived[Codec.Aux[GenericRecord, A]] = Derived {
    given ProductGeneric[A] = inst.generic
    given Labelling[A] = summonInline
    val nullDefaultBase: Boolean = summonFrom {
      case a: Annotation[AvroNullDefault, A] => a.apply().enabled
      case _ => false
    }
    dpImpl(nameOf[A], namespaceOf[A], docOf[A], nullDefaultBase)
  }

  private def dpImpl[A](name: String, namespace: String, doc: Option[String], nullDefaultBase: Boolean)(using nullDefaults: Annotations[AvroNullDefault, A], docs: Annotations[AvroDoc, A], inst: ProductInstances[Codec, A], l: Labelling[A]) = {
    // val schema = AvroError.catchNonFatal {
    //   type Acc = (Either[AvroError, Chain[Schema.Field]], List[String], List[Option[AvroNullDefault]], List[Option[AvroDoc]])
    //   val stuff = l
    //   inst.unfold0[Acc](Right(Chain.empty), l.elemLabels.toList, nullDefaults().toList.asInstanceOf, docs().toList.asInstanceOf) { [t] =>
    //     (acc: Acc, codec: Codec[t]) =>
    //       val newFields = (acc._1, codec.schema).mapN { (fields, schema) =>
    //         fields.append {
    //           new Schema.Field(
    //             acc._2.head,
    //             schema,
    //             acc._4.doc.orNull,
    //             default = {
    //               val fieldNullDefault: Option[Boolean] = acc._3.head.map(_.enabled)
    //               val wantNullDefault: Boolean = fieldNullDefault.getOrElse(nullDefaultBase)
    //               if (wantNullDefault && schema.isNullable) codec.decode(null, schema).toOption
    //               else None
    //             }
    //           )
    //         }
    //       }
    //       (newFields, acc._1.tail, acc._2.tail, acc._3.tail)
    //   }._1.map { 
    //     fields => 
    //       Schema.createRecord(
    //         name,
    //         doc.orNull,
    //         namespace,
    //         false,
    //         fields.toList.asJava
    //       )
    //   }
    //   Codec
    //     .instanceForTypes[GenericRecord, A](
    //       "IndexedRecord",
    //       "srta",
    //       schema,
    //       a =>
    //         schema.flatMap { schema =>
    //           type Acc = (Either[AvroError, Chain[(String, Any)]], List[String])
    //           val fields: Either[AvroError, Chain[(String, Any)]] =
    //             inst.foldLeft[Acc]((Right(Chain.empty), l.elemlabels.toList)) {
    //               [t] => (acc: Acc, codec: Codec[t], field: t) =>
    //                 (acc._1.flatMap { fds => 
    //                   codec.encode(t).map(result => (acc._2.head, result))
    //                 }, acc._2.tail)
    //               }

    //           fields.map { values =>
    //             val record = new GenericData.Record(schema)
    //             values.foldLeft(()) {
    //               case ((), (name, value)) => record.put(name, value)
    //             }
    //             record
    //           }
    //         }, {
    //         case (record: IndexedRecord, _) =>
    //           inst.
    //           free.foldMap {
    //             new (Field[A, *] ~> Either[AvroError, *]) {
    //               def apply[B](field: Field[A, B]): Either[AvroError, B] =
    //                 (field.name +: field.aliases.toList)
    //                   .collectFirstSome { name =>
    //                     Option(record.getSchema.getField(name))
    //                   }
    //                   .fold(field.default.toRight(AvroError.decodeMissingRecordField(field.name))) {
    //                     schemaField =>
    //                       field.codec.decode(record.get(schemaField.pos), schemaField.schema)
    //                   }
    //             }
    //           }
    //       }
    //     )
    // }
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
            inst.generic.fromRepr(Tuple.fromArray(as.toList.toArray).asInstanceOf)
          }
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