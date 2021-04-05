package vulcan.refined

import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.types.numeric.{NonPosInt, PosInt}
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan.Codec

final class RefinedSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("Refined") {
    it("should succeed for values conforming to predicate") {
      forAll { (posInt: PosInt) =>
        val codec = Codec[PosInt]
        val schema = codec.schema
        val encoded = codec.encode(posInt).value
        val decoded = codec.decode(encoded, schema).value
        assert(decoded === posInt)
      }
    }

    it("should fail for values not conforming to predicate") {
      forAll { (nonPosInt: NonPosInt) =>
        val codec = Codec[PosInt]
        val schema = codec.schema
        val encoded = Codec[Int].encode(nonPosInt.value).value
        val error = codec.decode(encoded, schema).swap.map(_.message).value
        assert(error === s"Predicate failed: ($nonPosInt > 0).")
      }
    }
  }
}
