package vulcan

import cats.Eq
import cats.laws.discipline.InvariantTests
import cats.tests.CatsSuite
import java.nio.charset.{Charset, StandardCharsets}
import org.apache.avro.{Schema}
import org.scalacheck.{Arbitrary, Gen}
import scala.util.Try

final class CodecInvariantSpec extends CatsSuite with EitherValues {
  val schemaGen: Gen[Either[AvroError, Schema]] =
    Gen.oneOf(
      Left(AvroError("error")),
      Codec[Int].schema,
      Codec[String].schema
    )

  val charsetGen: Gen[Charset] =
    Gen.oneOf(
      StandardCharsets.ISO_8859_1,
      StandardCharsets.US_ASCII,
      StandardCharsets.UTF_8,
      StandardCharsets.UTF_16
    )

  implicit val codecStringArbitrary: Arbitrary[Codec[String]] =
    Arbitrary {
      schemaGen.flatMap { schema =>
        charsetGen.map { charset =>
          Codec.instance(
            schema,
            s => Right(Avro.Bytes(java.nio.ByteBuffer.wrap(s.getBytes(charset)), None)),
            value => Right(new String(value.asInstanceOf[Avro.Bytes].value.array(), charset))
          )
        }
      }
    }

  implicit val codecStringEq: Eq[Codec[String]] =
    Eq.instance { (c1, c2) =>
      Try {
        forAll { (s: String) =>
          val e1 = c1.encode(s)
          val e2 = c2.encode(s)

          assert(e1.isRight == e2.isRight)
          if (e1.isRight && e2.isRight) {
            val d1 = c1.decode(e1.value)
            val d2 = c2.decode(e2.value)
            assert(d1 === d2)
          }
        }
      }.isSuccess
    }

  checkAll("Codec", InvariantTests[Codec].invariant[String, String, String])
}
