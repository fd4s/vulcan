package vulcan.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, Codec, Props}

sealed trait FixedBoolean extends Product with Serializable

object FixedBoolean {
  implicit val codec: Codec[FixedBoolean] =
    Codec
      .fixed(
        name = "FixedBoolean",
        namespace = "vulcan.examples",
        size = 1,
        encode = {
          case FalseFixedBoolean => Array(0.toByte)
          case TrueFixedBoolean  => Array(0.toByte, 1.toByte)
        },
        decode = bytes => {
          val byte = bytes.head
          if (byte == 0.toByte) Right(FalseFixedBoolean)
          else if (byte == 1.toByte) Right(TrueFixedBoolean)
          else Left(AvroError(s"unknown byte $byte"))
        },
        doc = Some("A boolean represented as a byte"),
        aliases = List("SomeOtherBoolean"),
        props = Props.one("custom", "value")
      )

  implicit val fixedBooleanEq: Eq[FixedBoolean] =
    Eq.fromUniversalEquals

  implicit val fixedBooleanArbitrary: Arbitrary[FixedBoolean] =
    Arbitrary(Gen.const(FalseFixedBoolean))
}

case object TrueFixedBoolean extends FixedBoolean

case object FalseFixedBoolean extends FixedBoolean
