/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroDoc("documentation")
final case class CaseClassAvroDoc(value: Option[String])

object CaseClassAvroDoc {
  implicit val codec: Codec[CaseClassAvroDoc] =
    Codec.derive
}
