package vulcan.generic

import vulcan.{BaseSpec, Codec}
import vulcan.generic.examples.CaseClassAvroNullDefault

final class AvroNullDefaultSpec extends BaseSpec {
  describe("AvroNullDefault") {
    it("should provide null default flag via enabled") {
      forAll { (b: Boolean) =>
        assert(new AvroNullDefault(b).enabled == b)
      }
    }

    it("should include enabled flag value in toString") {
      forAll { (b: Boolean) =>
        assert(new AvroNullDefault(b).toString.contains(b.toString))
      }
    }

    it("should provide an extractor for enabled flag") {
      forAll { (b1: Boolean) =>
        assert(new AvroNullDefault(b1) match {
          case AvroNullDefault(`b1`) => true
          case AvroNullDefault(b2)   => fail(b2.toString)
        })
      }
    }

    it("should decode null default when field is missing from writer schema") {
      case class WriterCaseClass(int: Int)

      implicit val writerCodec: Codec[WriterCaseClass] =
        Codec.record("CaseClassAvroNullDefault", "vulcan.generic.examples") { f =>
          f("int", _.int).map { WriterCaseClass(_) }
        }

      val encoded = Codec.toBinary(WriterCaseClass(3))
      val decoded =
        encoded.flatMap(Codec.fromBinary[CaseClassAvroNullDefault](_, writerCodec.schema))

      assert(decoded.value == CaseClassAvroNullDefault(Some(3), None, None, None, None, None, None))

    }
  }
}
