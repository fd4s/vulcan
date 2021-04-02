package vulcan.refined

import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.types.numeric.{NonPosInt, PosInt}
import munit.ScalaCheckSuite
import org.scalacheck.Prop._
import vulcan.Codec

final class RefinedSpec extends ScalaCheckSuite with EitherValues {
    property("Refined should succeed for values conforming to predicate") {
      forAll { (posInt: PosInt) =>
        val codec = Codec[PosInt]
        val schema = codec.schema.value
        val encoded = codec.encode(posInt).value
        val decoded = codec.decode(encoded, schema).value
        decoded == posInt
      }
    }

    property("Refined should fail for values not conforming to predicate") {
      forAll { (nonPosInt: NonPosInt) =>
        val codec = Codec[PosInt]
        val schema = codec.schema.value
        val encoded = Codec[Int].encode(nonPosInt.value).value
        val error = codec.decode(encoded, schema).swap.map(_.message).value
        error == s"Predicate failed: ($nonPosInt > 0)."
      }
    }
}
