/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroDoc("Some documentation")
final case class FixedAvroDoc(bytes: Array[Byte])

object FixedAvroDoc {
  implicit val codec: Codec[FixedAvroDoc] =
    deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
