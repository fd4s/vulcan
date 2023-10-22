package benchmarks

import cats.implicits._
import benchmarks.record._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import vulcan.Codec
import vulcan.generic._

object Encoding extends BenchmarkHelpers {

  @State(Scope.Thread)
  class Setup {
    val record = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))

    val specificRecord = {
      import benchmarks.record.generated.AttributeValue._
      import benchmarks.record.generated._
      new RecordWithUnionAndTypeField(new ValidInt(255, t))
    }

    // @nowarn
    implicit def attributeValueCodec: Codec[AttributeValue[Int]] = Codec.derive[AttributeValue[Int]]
    implicit val derivedCodec: Codec[RecordWithUnionAndTypeField] =
      Codec.derive[RecordWithUnionAndTypeField]

    val schema = derivedCodec.schema.leftMap(_.throwable).toTry.get
  }
}

class Encoding extends CommonParams with BenchmarkHelpers {

  import Encoding._

  @Benchmark
  def avroSpecificRecord(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(setup.specificRecord.toByteBuffer)

  @Benchmark
  def vulcanGenerated(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(Codec.toBinary(setup.record)(setup.derivedCodec))
}
