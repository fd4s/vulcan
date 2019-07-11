package vulcan

import vulcan.examples._
import org.scalatest.compatible.Assertion

final class PrismSpec extends BaseSpec {
  describe("Prism") {
    describe("derive") {
      it("partialRoundtrip") {
        def check[S, A](s: S, prism: Prism[S, A]): Assertion =
          assert(prism.getOption(s).fold(s)(prism.reverseGet) === s)

        forAll { s: SealedTraitCaseClass =>
          check(s, Prism[SealedTraitCaseClass, FirstInSealedTraitCaseClass])
          check(s, Prism[SealedTraitCaseClass, SecondInSealedTraitCaseClass])
          check(s, Prism[SealedTraitCaseClass, ThirdInSealedTraitCaseClass])
        }
      }

      it("roundtrip") {
        def check[S, A](a: A, prism: Prism[S, A]): Assertion =
          assert(prism.getOption(prism.reverseGet(a)) === Some(a))

        forAll { first: FirstInSealedTraitCaseClass =>
          check(first, Prism[SealedTraitCaseClass, FirstInSealedTraitCaseClass])
        }

        forAll { second: SecondInSealedTraitCaseClass =>
          check(second, Prism[SealedTraitCaseClass, SecondInSealedTraitCaseClass])
        }

        forAll { third: ThirdInSealedTraitCaseClass =>
          check(third, Prism[SealedTraitCaseClass, ThirdInSealedTraitCaseClass])
        }
      }
    }

    describe("instance") {
      it("toString") {
        assert {
          Prism[SealedTraitCaseClass, FirstInSealedTraitCaseClass]
            .toString()
            .startsWith("Prism$")
        }
      }
    }

    describe("partial") {
      it("getOption defined when get is defined") {
        val prism =
          Prism.partial[SealedTraitCaseClass, FirstInSealedTraitCaseClass] {
            case first @ FirstInSealedTraitCaseClass(_) => first
          }(identity)

        forAll { first: FirstInSealedTraitCaseClass =>
          assert { prism.getOption(first) === Some(first) }
        }

        forAll { second: SecondInSealedTraitCaseClass =>
          assert { prism.getOption(second) === None }
        }

        forAll { third: ThirdInSealedTraitCaseClass =>
          assert { prism.getOption(third) === None }
        }
      }
    }
  }
}
