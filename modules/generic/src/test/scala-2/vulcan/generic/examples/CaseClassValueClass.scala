/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassValueClass(value: Int) extends AnyVal

object CaseClassValueClass {
  implicit val codec: Codec[CaseClassValueClass] = Codec.derive
}
