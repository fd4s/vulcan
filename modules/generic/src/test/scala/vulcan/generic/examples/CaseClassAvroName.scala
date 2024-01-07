/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroName("CaseClassOtherName")
final case class CaseClassAvroName(
  @AvroName("otherValue")
  value: Option[String]
)

object CaseClassAvroName {
  implicit val codec: Codec[CaseClassAvroName] = Codec.derive
}
