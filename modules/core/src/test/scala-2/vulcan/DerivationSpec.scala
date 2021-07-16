package vulcan

import vulcan.examples._

final class DerivationSpec extends BaseSpec with RoundtripHelpers with CodecSpecHelpers {
  describe("Codec") {
    describe("deriveEnum") {
      describe("schema") {
        it("should derive name, namespace, doc") {
          assertSchemaIs[CaseClassAvroDoc] {
            """{"type":"enum","name":"CaseClassAvroDoc","namespace":"vulcan.examples","doc":"documentation","symbols":["first"]}"""
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
            """{"type":"fixed","name":"FixedAvroDoc","namespace":"vulcan.examples","doc":"Some documentation","size":1}"""
          }
        }

        it("should use namespace annotation") {
          assertSchemaIs[FixedNamespace] {
            """{"type":"fixed","name":"FixedNamespace","namespace":"vulcan.examples.overridden","doc":"Some documentation","size":1}"""
          }
        }
      }
      it("should roundtrip") { roundtrip[FixedNamespace] }

    }
  }
}
