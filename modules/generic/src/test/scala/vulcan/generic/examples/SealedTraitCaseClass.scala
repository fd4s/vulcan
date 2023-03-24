/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseClass

final case class CaseClassInSealedTrait(value: Int) extends SealedTraitCaseClass

object CaseClassInSealedTrait {
  implicit val codec: Codec[CaseClassInSealedTrait] =
    Codec.derive
}

object SealedTraitCaseClass {
  implicit val codec: Codec[SealedTraitCaseClass] =
    Codec.derive
}
