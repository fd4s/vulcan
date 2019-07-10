package vulcan

final class PackageSpec extends BaseSpec {
  describe("Package") {
    describe("encode") {
      it("should encode using codec for type") {
        forAll { n: Int =>
          assert(encode(n).value === n)
        }
      }
    }

    describe("decode") {
      it("should decode using codec for type") {
        forAll { n: Int =>
          assert(decode[Int](n).value === n)
        }
      }
    }
  }
}
