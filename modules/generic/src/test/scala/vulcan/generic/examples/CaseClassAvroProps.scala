/*
 * Copyright 2019-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.{Codec, Props}
import vulcan.generic._

@AvroName("CaseClassAvroProps")
final case class CaseClassAvroProps(
  @AvroName("value")
  @AvroProps(Props.one("prop1", "A").add("prop2", "B"))
  value: Option[String]
)

object CaseClassAvroProps {
  implicit val codec: Codec[CaseClassAvroProps] = Codec.derive
}
