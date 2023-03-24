/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.syntax.show._
import scala.annotation.nowarn

final class PropsSpec extends BaseSpec {
  @nowarn("cat=deprecation") // TODO
  val codecSchemaError: Codec[Int] =
    Codec.instance(
      schema = Left(AvroError("error")),
      encode = _ => Left(AvroError("error")),
      decode = Codec.int.decode
    )

  describe("Props") {
    describe("one") {
      describe("add") {
        describe("error") {
          it("toChain") {
            assert {
              Props
                .one("first", "firstValue")
                .add("second", 2)(codecSchemaError)
                .toChain
                .isLeft
            }
          }

          it("toString") {
            assert {
              Props
                .one("first", "firstValue")
                .add("second", 2)(codecSchemaError)
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
                  .one("name", 1)(codecSchemaError)
                  .add("name", 1)(codecSchemaError)
                  .toChain
                  .swap
                  .value
                  .message == "error"
              }
            }

            it("toString") {
              assert {
                Props
                  .one("name", 1)(codecSchemaError)
                  .add("name", 1)(codecSchemaError)
                  .toString == "AvroError(error)"
              }
            }
          }

          it("toChain") {
            assert {
              Props
                .one("name", 1)(codecSchemaError)
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
                .one("name", 1)(codecSchemaError)
                .add("name", 1)
                .toString == "AvroError(error)"
            }
          }
        }

        it("toChain") {
          assert {
            Props
              .one("name", 1)(codecSchemaError)
              .toChain
              .swap
              .value
              .message == "error"
          }
        }

        it("toString") {
          assert {
            Props
              .one("name", 1)(codecSchemaError)
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
            .add("name", 1)(codecSchemaError)
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
            .one("name", 1)(codecSchemaError)
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
            .add("name", 2)(codecSchemaError)
            .show == "AvroError(error)"
        }
      }

      it("one.error.add") {
        assert {
          Props
            .one("name", 1)(codecSchemaError)
            .add("name", 2)
            .show == "AvroError(error)"
        }
      }

      it("one.error.add.error") {
        assert {
          Props
            .one("name", 1)(codecSchemaError)
            .add("name", 2)(codecSchemaError)
            .show == """AvroError(error)"""
        }
      }
    }
  }
}
