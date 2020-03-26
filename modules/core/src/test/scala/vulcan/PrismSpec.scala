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

    describe("identity") {
      it("roundtrip") {
        def check[S, A](a: A, prism: Prism[S, A]): Assertion =
          assert(prism.getOption(prism.reverseGet(a)) === Some(a))

        forAll { n: Int =>
          check(n, Prism.identity[Int])
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

    describe("none") {
      it("none") {
        assert(Prism[Option[Int], None.type].getOption(None) == Some(None))
      }

      it("some") {
        forAll { n: Int =>
          assert {
            Prism[Option[Int], None.type].getOption(Some(n)) == None
          }
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

    describe("some") {
      it("none") {
        assert(Prism[Option[Int], Some[Int]].getOption(None) == None)
      }

      it("some") {
        forAll { n: Int =>
          assert(Prism[Option[Int], Some[Int]].getOption(Some(n)) == Some(Some(n)))
        }
      }

      it("some.coproduct") {
        sealed trait FirstOrSecond
        final case class First(a: Int) extends FirstOrSecond
        final case class Second(b: Double) extends FirstOrSecond

        assert(Prism[Option[FirstOrSecond], Some[First]].getOption(None) == None)

        forAll { n: Double =>
          assert {
            Prism[Option[FirstOrSecond], Some[First]]
              .getOption(Some(Second(n))) == None
          }
        }

        forAll { n: Int =>
          assert {
            Prism[Option[FirstOrSecond], Some[First]]
              .getOption(Some(First(n))) == Some(Some(First(n)))
          }
        }
      }
    }
  }
}
