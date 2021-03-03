package vulcan

import scala.annotation.nowarn

@nowarn("msg=deprecated")
final class AvroDocSpec extends BaseSpec {
  describe("AvroDoc") {
    it("should provide documentation via doc") {
      forAll { (s: String) =>
        assert(new AvroDoc(s).doc == s)
      }
    }

    it("should include documentation in toString") {
      forAll { (s: String) =>
        assert(new AvroDoc(s).toString.contains(s))
      }
    }

    it("should provide an extractor for documentation") {
      forAll { (s1: String) =>
        assert(new AvroDoc(s1) match {
          case AvroDoc(`s1`) => true
          case AvroDoc(s2)   => fail(s2)
        })
      }
    }
  }
}
