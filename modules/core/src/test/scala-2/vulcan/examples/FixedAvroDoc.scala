/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import scala.annotation.nowarn
import vulcan.{AvroDoc, Codec}

@AvroDoc("Some documentation")
@nowarn("msg=deprecated")
final case class FixedAvroDoc(bytes: Array[Byte])

object FixedAvroDoc {
  @nowarn("msg=deprecated")
  implicit val codec: Codec[FixedAvroDoc] =
    Codec.deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
