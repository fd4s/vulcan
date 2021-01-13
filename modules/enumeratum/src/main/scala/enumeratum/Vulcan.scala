/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum

import scala.reflect.runtime.universe.WeakTypeTag
import vulcan.{AvroError, Codec}

object Vulcan {
  def enumCodec[A <: EnumEntry](
    enum: Enum[A]
  )(implicit tag: WeakTypeTag[A]): Codec[A] = {
    lazy val typeName = tag.tpe.typeSymbol.name.decodedName
    lazy val entries = enum.values.map(_.entryName).mkString(", ")
    val notFound = (name: String) => AvroError(s"$name is not a member of $typeName ($entries)")

    Codec.deriveEnum(
      symbols = enum.values.map(_.entryName),
      encode = _.entryName,
      decode = name => enum.withNameOption(name).toRight(notFound(name))
    )
  }
}
