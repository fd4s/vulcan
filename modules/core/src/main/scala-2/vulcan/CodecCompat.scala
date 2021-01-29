/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import org.apache.avro.generic.GenericFixed
import scala.reflect.runtime.universe.WeakTypeTag
import vulcan.internal.tags._

private[vulcan] trait CodecCompanionCompat {

  @deprecated(
    "Use vulcan.generic.deriveEnum in the vulcan-generic module, " +
      "or define the codec explicitly using Codec.enumeration",
    "1.3.0"
  )
  final def deriveEnum[A](
    symbols: Seq[String],
    encode: A => String,
    decode: String => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[AnyRef, A] =
    Codec.enumeration(
      name = nameFrom(tag),
      symbols = symbols,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )

  @deprecated(
    "Use vulcan.generic.deriveFixed in the vulcan-generic module, " +
      "or define the codec explicitly using Codec.enumeration",
    "1.3.0"
  )
  final def deriveFixed[A](
    size: Int,
    encode: A => Array[Byte],
    decode: Array[Byte] => Either[AvroError, A]
  )(implicit tag: WeakTypeTag[A]): Codec.Aux[GenericFixed, A] =
    Codec.fixed(
      name = nameFrom(tag),
      size = size,
      encode = encode,
      decode = decode,
      namespace = namespaceFrom(tag),
      doc = docFrom(tag)
    )
}
