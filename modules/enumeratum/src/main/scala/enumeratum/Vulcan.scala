/*
 * Copyright 2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
