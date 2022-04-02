/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.syntax.either._
import eu.timepit.refined.api.{RefType, Validate}

package object refined {
  implicit def refinedCodec[F[_, _], T, P](
    implicit codec: Codec[T],
    validate: Validate[T, P],
    refType: RefType[F]
  ): Codec[F[T, P]] = {
    val refine = refType.refine[P]
    codec.imapError(refine(_).leftMap(AvroError(_)))(refType.unwrap)
  }
}
