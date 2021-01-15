package vulcan.generic.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.Codec
import vulcan.generic._

@AvroDoc("Some documentation")
@AvroNamespace("vulcan.generic.examples.overridden")
final case class FixedNamespace(bytes: Array[Byte])
object FixedNamespace {
  implicit val codec: Codec[FixedNamespace] =
    deriveFixed(
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
