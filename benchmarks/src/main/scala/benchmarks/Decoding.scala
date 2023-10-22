package benchmarks

import cats.syntax.all._
import benchmarks.record._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import vulcan.Codec
import vulcan.generic._
import scala.annotation.nowarn

object Decoding extends BenchmarkHelpers {
  @State(Scope.Thread)
  class Setup {
    val avroBytes = {
      import benchmarks.record.generated.AttributeValue._
      import benchmarks.record.generated._
      new RecordWithUnionAndTypeField(new ValidInt(255, t)).toByteBuffer
    }

    @nowarn
    implicit private def attributeValueCodec: Codec[AttributeValue[Int]] =
      Codec.derive[AttributeValue[Int]]

    implicit val derivedCodec: Codec[RecordWithUnionAndTypeField] =
      Codec.derive[RecordWithUnionAndTypeField]

    val schema = derivedCodec.schema.leftMap(_.throwable).toTry.get

    val vulcanBytes = Codec
      .toBinary(RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t)))
      .leftMap(_.throwable)
      .toTry
      .get
  }
}

class Decoding extends CommonParams with BenchmarkHelpers {

  import Decoding._

  @Benchmark
  def avroSpecificRecord(setup: Setup, blackhole: Blackhole) = {
    import benchmarks.record.generated._
    blackhole.consume(RecordWithUnionAndTypeField.fromByteBuffer(setup.avroBytes.duplicate))
  }

  @Benchmark
  def vulcanGenerated(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(
      Codec.fromBinary[RecordWithUnionAndTypeField](setup.vulcanBytes, setup.schema)(
        setup.derivedCodec
      )
    )
}
