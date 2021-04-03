package vulcan

import cats.Eq
import cats.laws.discipline.InvariantTests
import cats.tests.CatsSuite
import java.nio.charset.{Charset, StandardCharsets}
import org.apache.avro.Schema
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
          Codec
            .instance[AnyRef, String](
              schema,
              s => Right(s.getBytes(charset)),
              (value, schema) => {
                if (schema.getType() == Schema.Type.STRING)
                  Right(new String(value.asInstanceOf[Array[Byte]], charset))
                else
                  Left {
                    AvroError
                      .decodeUnexpectedSchemaType(
                        schema.getType(),
                        Schema.Type.STRING
                      )
                  }
              }
            )
            .adaptDecodeError(AvroError.errorDecodingTo("String", _))
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
            val d1 = c1.schema.flatMap(c1.decode(e1.value, _))
            val d2 = c2.schema.flatMap(c2.decode(e2.value, _))
            assert(d1 === d2)
          }
        }
      }.isSuccess
    }

  checkAll("Codec", InvariantTests[Codec].invariant[String, String, String])
}
