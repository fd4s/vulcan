/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

final class AvroExceptionSpec extends BaseSpec {
  describe("AvroException") {
    it("should use specified message for the exception") {
      forAll { (message: String) =>
        assert(AvroException(message).getMessage() === message)
      }
    }

    it("should include the message in toString") {
      forAll { (message: String) =>
        assert(AvroException(message).toString() === s"vulcan.AvroException: $message")
      }
    }
  }
}
