package vulcan.generic

import vulcan.BaseSpec

final class AvroNullDefaultSpec extends BaseSpec {
  describe("AvroNullDefault") {
    it("should provide null default flag via enabled") {
      forAll { (b: Boolean) =>
        assert(new AvroNullDefault(b).enabled == b)
      }
    }

    it("should include enabled flag value in toString") {
      forAll { (b: Boolean) =>
        assert(new AvroNullDefault(b).toString.contains(b.toString))
      }
    }

    it("should provide an extractor for enabled flag") {
      forAll { (b1: Boolean) =>
        assert(new AvroNullDefault(b1) match {
          case AvroNullDefault(`b1`) => true
          case AvroNullDefault(b2)   => fail(b2.toString)
        })
      }
    }
  }
}
