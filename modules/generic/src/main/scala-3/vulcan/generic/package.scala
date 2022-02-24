/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import org.apache.avro.generic._
import org.apache.avro.Schema
import shapeless3.deriving._
import scala.compiletime._
import scala.reflect.ClassTag

package object generic {

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
