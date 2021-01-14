package enumeratum

import enumeratum.EnumEntry.Lowercase
import enumeratum.VulcanEnumSpec.Suit
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan.Codec
import vulcan.generic.{AvroDoc, AvroNamespace}

final class VulcanEnumSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("VulcanEnum") {
    describe("schema") {
      it("should use an enum schema") {
        assert {
          Codec[Suit].schema.value.toString ===
            """{"type":"enum","name":"Suit","namespace":"com.example","doc":"The different card suits","symbols":["clubs","diamonds","hearts","spades"]}"""
        }
      }
    }

    it("should roundtrip enum values") {
      val gen = Gen.oneOf[Suit](Suit.Diamonds, Suit.Hearts, Suit.Spades)
      forAll(gen) { suit =>
        val roundtrip = Codec.encode(suit).flatMap(Codec.decode[Suit])
        assert(roundtrip.value === suit)
      }
    }

    it("should error if withNameOption does not handle schema symbol") {
      val roundtrip = Codec.encode[Suit](Suit.Clubs).flatMap(Codec.decode[Suit])
      assert {
        roundtrip.swap.value.message ===
          "clubs is not a member of Suit (clubs, diamonds, hearts, spades)"
      }
    }
  }
}

object VulcanEnumSpec {

  @AvroNamespace("com.example")
  @AvroDoc("The different card suits")
  sealed trait Suit extends EnumEntry with Lowercase

  object Suit extends Enum[Suit] with VulcanEnum[Suit] {
    case object Clubs extends Suit
    case object Diamonds extends Suit
    case object Hearts extends Suit
    case object Spades extends Suit

    val values = findValues

    override def withNameOption(name: String): Option[Suit] =
      if (name == "clubs") None
      else super.withNameOption(name)
  }
}
