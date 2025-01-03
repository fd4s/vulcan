/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroAlias("FixedOtherAlias")
final case class FixedAvroAlias(bytes: Array[Byte])

object FixedAvroAlias {
  implicit val codec: Codec[FixedAvroAlias] =
    deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
