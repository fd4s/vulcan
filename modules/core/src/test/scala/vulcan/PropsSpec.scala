package vulcan

import cats.syntax.show._

final class PropsSpec extends BaseSpec {

  describe("Props") {
    describe("one") {
      describe("add") {

        it("toChain") {
          assert {
            Props
              .one("first", "firstValue")
              .add("second", "secondValue")
              .toChain
              .exists(_.size == 2)
          }
        }

        it("toString") {
          assert {
            Props
              .one("first", "firstValue")
              .add("second", "secondValue")
              .toString == "Props(first -> firstValue, second -> secondValue)"
          }
        }
      }

      describe("error") {
        describe("add") {}

      }

      it("toChain") {
        assert {
          Props.one("name", "value").toChain.exists(_.size == 1)
        }
      }

      it("toString") {
        assert {
          Props.one("name", "value").toString == "Props(name -> value)"
        }
      }
    }

    describe("empty") {
      describe("add") {
        it("toChain") {
          assert {
            Props.empty.add("name", "value").toChain.exists(_.size == 1)
          }
        }

        it("toString") {
          assert {
            Props.empty.add("name", "value").toString == "Props(name -> value)"
          }
        }
      }

      it("toChain") {
        assert {
          Props.empty.toChain.exists(_.isEmpty)
        }
      }

      it("toString") {
        assert {
          Props.empty.toString() == "Props()"
        }
      }
    }

    describe("show") {
      it("empty") {
        assert {
          Props.empty.show == "Props()"
        }
      }

      it("empty.add") {
        assert {
          Props.empty.add("name", "value").show == "Props(name -> value)"
        }
      }

      it("one") {
        assert {
          Props.one("name", "value").show == "Props(name -> value)"
        }
      }

      it("one.add") {
        assert {
          Props
            .one("first", "firstValue")
            .add("second", "secondValue")
            .show == "Props(first -> firstValue, second -> secondValue)"
        }
      }
    }
  }
}
