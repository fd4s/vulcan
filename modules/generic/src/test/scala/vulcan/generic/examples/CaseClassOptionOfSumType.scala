/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

case class CaseClassOptionOfSumType(field1: Option[SealedTraitCaseClassCustom])

object CaseClassOptionOfSumType {
  implicit val codec: Codec[CaseClassOptionOfSumType] = Codec.derive
}
