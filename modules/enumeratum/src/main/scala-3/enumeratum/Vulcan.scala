/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum

import vulcan.{Avro, AvroError, Codec}
import vulcan.internal.Names
import vulcan.generic.deriveEnum

object Vulcan {
  def enumCodec[A <: EnumEntry](
    `enum`: Enum[A]
  )(implicit names: Names[A]): Codec.Aux[Avro.EnumSymbol, A] = {
    lazy val entries = `enum`.values.map(_.entryName).mkString(", ")
    val notFound = (name: String) =>
      AvroError(s"$name is not a member of ${names.typeName} ($entries)")

    deriveEnum(
      symbols = `enum`.values.map(_.entryName),
      encode = _.entryName,
      decode = name => `enum`.withNameOption(name).toRight(notFound(name))
    )
  }
}
