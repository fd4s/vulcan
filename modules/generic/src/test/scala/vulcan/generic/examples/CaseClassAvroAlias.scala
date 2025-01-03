/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroAlias("CaseClassOtherAlias")
final case class CaseClassAvroAlias(
  @AvroAlias("otherValueAlias")
  value: Option[String]
)

object CaseClassAvroAlias {
  implicit val codec: Codec[CaseClassAvroAlias] = Codec.derive
}
