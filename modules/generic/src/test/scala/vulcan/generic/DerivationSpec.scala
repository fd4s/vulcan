/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.generic.examples._
import vulcan.BaseSpec
import vulcan.RoundtripHelpers
import vulcan.CodecSpecHelpers

final class DerivationSpec extends BaseSpec with RoundtripHelpers with CodecSpecHelpers {
  describe("Codec") {
    describe("deriveEnum") {
      describe("schema") {
        it("should derive name, namespace, doc") {
          assertSchemaIs[CaseClassEnumAvroDoc] {
            """{"type":"enum","name":"CaseClassEnumAvroDoc","namespace":"vulcan.generic.examples","doc":"documentation","symbols":["first"]}"""
          }
        }

        it("should use namespace annotation") {
          assertSchemaIs[SealedTraitEnumDerived] {
            """{"type":"enum","name":"SealedTraitEnumDerived","namespace":"com.example","symbols":["first","second"]}"""
          }
        }
      }

      it("should roundtrip") { roundtrip[SealedTraitEnumDerived] }
    }

    describe("deriveFixed") {
      describe("schema") {
        it("should derive name, namespace, doc") {
          assertSchemaIs[FixedAvroDoc] {
            """{"type":"fixed","name":"FixedAvroDoc","namespace":"vulcan.generic.examples","doc":"Some documentation","size":1}"""
          }
        }

        it("should derive overridden name") {
          assertSchemaIs[FixedAvroName] {
            """{"type":"fixed","name":"FixedOtherName","namespace":"vulcan.generic.examples","size":1}"""
          }
        }

        it("should use alias annotation") {
          assertSchemaIs[FixedAvroAlias] {
            """{"type":"fixed","name":"FixedAvroAlias","namespace":"vulcan.generic.examples","size":1,"aliases":["FixedOtherAlias"]}"""
          }
        }

        it("should use namespace annotation") {
          assertSchemaIs[FixedNamespace] {
            """{"type":"fixed","name":"FixedNamespace","namespace":"vulcan.generic.examples.overridden","doc":"Some documentation","size":1}"""
          }
        }
      }
      it("should roundtrip") { roundtrip[FixedNamespace] }

    }
  }
}
