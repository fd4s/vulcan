/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseObject

case object CaseObjectInSealedTrait extends SealedTraitCaseObject

object SealedTraitCaseObject {
  implicit val codec: Codec[SealedTraitCaseObject] =
    Codec.derive
}
