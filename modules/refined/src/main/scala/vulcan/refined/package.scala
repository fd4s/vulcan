/*
 * Copyright 2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
