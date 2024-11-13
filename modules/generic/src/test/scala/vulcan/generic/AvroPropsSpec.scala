/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.{BaseSpec, Props}
final class AvroPropsSpec extends BaseSpec {
  describe("AvroProps") {
    it("should provide props via props") {
      forAll { (name: String, value: String) =>
        assert(new AvroProps(Props.one(name, value)).prop.toChain == Props.one(name, value).toChain)
      }
    }

    it("should include name and value in props toString") {
      forAll { (name: String, value: String) =>
        val propsString = new AvroProps(Props.one(name, value)).toString
        assert(propsString.contains(name) && propsString.contains(value))
      }
    }

    it("should provide an extractor for props") {
      forAll { (name: String, value: String) =>
        assert(new AvroProps(Props.one(name, value)) match {
          case AvroProps(props) =>
            props.toChain.toOption.exists(_.exists { case (n, _) => n == name })
          case _ => false
        })
      }
    }
  }
}
