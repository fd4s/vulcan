package vulcan

final class AvroErrorSpec extends BaseSpec {
  describe("AvroError") {
    describe("throwable") {
      it("should include the message in getMessage") {
        forAll { (message: String) =>
          assert(AvroError(message).throwable.getMessage().contains(message))
        }
      }

      it("should include the message in toString") {
        forAll { (message: String) =>
          assert(AvroError(message).throwable.toString().contains(message))
        }
      }
    }
  }
}
