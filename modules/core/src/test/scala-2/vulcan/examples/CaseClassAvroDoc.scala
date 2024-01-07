/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import vulcan.{AvroDoc, Codec}
import scala.annotation.nowarn

@AvroDoc("documentation")
@nowarn("msg=deprecated")
final case class CaseClassAvroDoc(value: Option[String])

object CaseClassAvroDoc {
  @nowarn("msg=deprecated")
  implicit val codec: Codec[CaseClassAvroDoc] =
    Codec.deriveEnum(
      symbols = List("first"),
      encode = _ => "first",
      decode = _ => Right(CaseClassAvroDoc(None))
    )
}
