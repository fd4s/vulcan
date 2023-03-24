/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import scala.annotation.nowarn
import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroDoc, AvroNamespace, Codec}

@AvroDoc("Some documentation")
@AvroNamespace("vulcan.examples.overridden")
@nowarn("msg=deprecated")
final case class FixedNamespace(bytes: Array[Byte])
object FixedNamespace {
  @nowarn("msg=deprecated")
  implicit val codec: Codec[FixedNamespace] =
    Codec.deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(FixedNamespace(bytes))
    )

  implicit val fixedNamespaceEq: Eq[FixedNamespace] =
    Eq.instance { (f1, f2) =>
      val b1 = f1.bytes
      val b2 = f2.bytes

      b1.length == b2.length && b1.zip(b2).forall {
        case (a, b) => a == b
      }
    }

  implicit val fixedNamespaceArbitrary: Arbitrary[FixedNamespace] =
    Arbitrary {
      Gen
        .choose(Byte.MinValue, Byte.MaxValue)
        .map(byte => FixedNamespace(Array[Byte](byte)))
    }
}
