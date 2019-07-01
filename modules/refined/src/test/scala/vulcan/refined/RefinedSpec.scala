package vulcan.refined

import cats.syntax.either._
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.types.numeric.{NonPosInt, PosInt}
import org.scalatest.EitherValues._
import org.scalatest.FunSpec
import org.scalatest.prop.PropertyChecks
import vulcan.Codec

final class RefinedSpec extends FunSpec with PropertyChecks {
  describe("Refined") {
    it("should succeed for values conforming to predicate") {
      forAll { posInt: PosInt =>
        val codec = Codec[PosInt]
        val schema = codec.schema.right.value
        val encoded = codec.encode(posInt, schema).right.value
        val decoded = codec.decode(encoded, schema).right.value
        assert(decoded === posInt)
      }
    }

    it("should fail for values not conforming to predicate") {
      forAll { nonPosInt: NonPosInt =>
        val codec = Codec[PosInt]
        val schema = codec.schema.right.value
        val encoded = Codec[Int].encode(nonPosInt.value, schema).right.value
        val error = codec.decode(encoded, schema).leftMap(_.message).left.value
        assert(error === s"Predicate failed: ($nonPosInt > 0).")
      }
    }
  }
}
