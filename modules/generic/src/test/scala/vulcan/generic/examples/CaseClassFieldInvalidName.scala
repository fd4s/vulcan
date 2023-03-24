/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassFieldInvalidName(`-value`: Int)

object CaseClassFieldInvalidName {
  implicit val codec: Codec[CaseClassFieldInvalidName] =
    Codec.derive
}
