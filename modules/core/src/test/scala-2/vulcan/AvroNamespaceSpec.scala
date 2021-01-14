package vulcan

final class AvroNamespaceSpec extends BaseSpec {
  describe("AvroNamespace") {
    it("should provide the namespace via namespace") {
      forAll { (s: String) =>
        assert(new AvroNamespace(s).namespace == s)
      }
    }

    it("should include namespace in toString") {
      forAll { (s: String) =>
        assert(new AvroNamespace(s).toString.contains(s))
      }
    }

    it("should provide an extractor for namespace") {
      forAll { (s1: String) =>
        assert(new AvroNamespace(s1) match {
          case AvroNamespace(`s1`) => true
          case AvroNamespace(s2)   => fail(s2)
        })
      }
    }
  }
}
