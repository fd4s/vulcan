package vulcan

final class AvroExceptionSpec extends BaseSpec {
  describe("AvroException") {
    it("should use specified message for the exception") {
      forAll { message: String =>
        assert(AvroException(message).getMessage() === message)
      }
    }

    it("should include the message in toString") {
      forAll { message: String =>
        assert(AvroException(message).toString() === s"vulcan.AvroException: $message")
      }
    }
  }
}
