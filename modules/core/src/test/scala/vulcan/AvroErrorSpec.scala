/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

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
