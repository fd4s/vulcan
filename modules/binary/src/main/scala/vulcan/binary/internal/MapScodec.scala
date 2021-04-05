package vulcan.binary.internal

import cats.data.Chain
import org.apache.avro.util.Utf8
import scodec.bits.BitVector
import scodec.{Attempt, DecodeResult, SizeBound, codecs, Codec => Scodec}
import vulcan.binary.intScodec
import vulcan.internal.converters.collection._

import java.util
import scala.annotation.tailrec

private[binary] class MapScodec[A](codec: Scodec[A]) extends Scodec[util.Map[Utf8, A]] {
  override def decode(bits: BitVector): Attempt[DecodeResult[util.Map[Utf8, A]]] = {
    def decodeBlock(bits: BitVector): Attempt[DecodeResult[List[(Utf8, A)]]] =
      codecs.listOfN(ZigZagVarIntCodec, vulcan.binary.stringScodec.flatZip(_ => codec)).decode(bits)

    @tailrec def loop(
      remaining: BitVector,
      acc: Chain[(Utf8, A)]
    ): Attempt[DecodeResult[Chain[(Utf8, A)]]] =
      decodeBlock(remaining) match {
        case Attempt.Successful(DecodeResult(value, remainder)) =>
          if (value.isEmpty) Attempt.successful(DecodeResult(acc, remainder))
          else loop(remainder, acc ++ Chain.fromSeq(value))
        case f: Attempt.Failure => f
      }

    loop(bits, Chain.empty).map(_.map(_.toList.toMap.asJava))
  }

  override def encode(value: util.Map[Utf8, A]): Attempt[BitVector] = {
    intScodec.encode(value.size).flatMap { bits =>
      codecs
        .list(vulcan.binary.stringScodec.flatZip(_ => codec))
        .encode(value.asScala.toList)
        .map(bits ++ _)
        .flatMap(
          bits =>
            (if (value.size != 0) intScodec.encode(0)
             else Attempt.successful(BitVector.empty)).map(bits ++ _)
        )
    }
  }
  override val sizeBound: SizeBound = intScodec.sizeBound.atLeast
}
