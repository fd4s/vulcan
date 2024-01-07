/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassValueClass(value: Int) extends AnyVal

object CaseClassValueClass {
  // we don't support autoderivation for value classes in Scala 3
  implicit val codec: Codec[CaseClassValueClass] = Codec[Int].imap(apply)(_.value)
}
