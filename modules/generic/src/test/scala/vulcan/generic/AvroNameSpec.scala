/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.BaseSpec
final class AvroNameSpec extends BaseSpec {
  describe("AvroName") {
    it("should provide name via name") {
      forAll { (s: String) =>
        assert(new AvroName(s).name == s)
      }
    }

    it("should include name in toString") {
      forAll { (s: String) =>
        assert(new AvroName(s).toString.contains(s))
      }
    }

    it("should provide an extractor for documentation") {
      forAll { (s1: String) =>
        assert(new AvroName(s1) match {
          case AvroName(`s1`) => true
          case AvroName(s2)   => fail(s2)
        })
      }
    }
  }
}
