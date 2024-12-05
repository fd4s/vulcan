/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.BaseSpec

final class AvroAliasSpec extends BaseSpec {
  describe("AvroAlias") {
    it("should provide alias via alias") {
      forAll { (s: String) =>
        assert(new AvroAlias(s).alias == s)
      }
    }

    it("should include alias in toString") {
      forAll { (s: String) =>
        assert(new AvroAlias(s).toString.contains(s))
      }
    }

    it("should provide an extractor for alias") {
      forAll { (s1: String) =>
        assert(new AvroAlias(s1) match {
          case AvroAlias(`s1`) => true
          case AvroAlias(s2)   => fail(s2)
        })
      }
    }
  }
}
