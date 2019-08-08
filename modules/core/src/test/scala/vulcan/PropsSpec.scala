package vulcan

import cats.syntax.show._

final class PropsSpec extends BaseSpec {
  describe("Props") {
    describe("one") {
      describe("add") {
        describe("error") {
          it("toChain") {
            assert {
              Props
                .one("first", "firstValue")
                .add("second", 2)(Codec.int.withSchema(Left(AvroError("error"))))
                .toChain
                .isLeft
            }
          }

          it("toString") {
            assert {
              Props
                .one("first", "firstValue")
                .add("second", 2)(Codec.int.withSchema(Left(AvroError("error"))))
                .toString == "AvroError(error)"
            }
          }
        }

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
        describe("add") {
          describe("error") {
            it("toChain") {
              assert {
                Props
                  .one("name", 1)(Codec.int.withSchema(Left(AvroError("error1"))))
                  .add("name", 1)(Codec.int.withSchema(Left(AvroError("error2"))))
                  .toChain
                  .swap
                  .value
                  .message == "error1"
              }
            }

            it("toString") {
              assert {
                Props
                  .one("name", 1)(Codec.int.withSchema(Left(AvroError("error1"))))
                  .add("name", 1)(Codec.int.withSchema(Left(AvroError("error2"))))
                  .toString == "AvroError(error1)"
              }
            }
          }

          it("toChain") {
            assert {
              Props
                .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
                .add("name", 1)
                .toChain
                .swap
                .value
                .message == "error"
            }
          }

          it("toString") {
            assert {
              Props
                .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
                .add("name", 1)
                .toString == "AvroError(error)"
            }
          }
        }

        it("toChain") {
          assert {
            Props
              .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
              .toChain
              .swap
              .value
              .message == "error"
          }
        }

        it("toString") {
          assert {
            Props
              .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
              .toString == "AvroError(error)"
          }
        }
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

      it("empty.add.error") {
        assert {
          Props.empty
            .add("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
            .show == "AvroError(error)"
        }
      }

      it("one") {
        assert {
          Props.one("name", "value").show == "Props(name -> value)"
        }
      }

      it("one.error") {
        assert {
          Props
            .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
            .show == "AvroError(error)"
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

      it("one.add.error") {
        assert {
          Props
            .one("name", 1)
            .add("name", 2)(Codec.int.withSchema(Left(AvroError("error"))))
            .show == "AvroError(error)"
        }
      }

      it("one.error.add") {
        assert {
          Props
            .one("name", 1)(Codec.int.withSchema(Left(AvroError("error"))))
            .add("name", 2)
            .show == "AvroError(error)"
        }
      }

      it("one.error.add.error") {
        assert {
          Props
            .one("name", 1)(Codec.int.withSchema(Left(AvroError("error1"))))
            .add("name", 2)(Codec.int.withSchema(Left(AvroError("error2"))))
            .show == """AvroError(error1)"""
        }
      }
    }
  }
}
