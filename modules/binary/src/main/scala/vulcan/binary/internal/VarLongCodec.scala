package vulcan.binary.internal

import scodec.bits.BitVector
import scodec.{Attempt, DecodeResult, Err, SizeBound, Codec => Scodec}

import java.nio.{ByteBuffer, ByteOrder}
import scala.annotation.tailrec

object VarLongCodec extends Scodec[Long] {

  override def sizeBound = SizeBound.bounded(1L, 11L)

  override def encode(i: Long): Attempt[BitVector] = {
    val buffer = ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN)
    val written = runEncodingBE(value, buffer, 8)
    buffer.flip()
    val relevantBits = BitVector.view(buffer).take(written.toLong)
    Attempt.successful(relevantBits)
  }

  override def decode(buffer: BitVector): Attempt[DecodeResult[Long]] =
    runDecodingBE(buffer, MostSignificantBit.toByte, 0L, 0)

  override def toString = "variable-length integer"

  @tailrec
  private def runEncodingBE(value: Long, buffer: ByteBuffer, size: Int): Int =
    if ((value & MoreBytesMask) != 0) {
      buffer.put((value & RelevantDataBits | MostSignificantBit).toByte)
      runEncodingBE(value >>> BitsPerByte, buffer, size + 8)
    } else {
      buffer.put(value.toByte)
      size
    }

  @tailrec
  private def runDecodingBE(
    buffer: BitVector,
    byte: Byte,
    value: Long,
    shift: Int
  ): Attempt[DecodeResult[Long]] =
    if ((byte & MostSignificantBit) != 0) {
      if (buffer.sizeLessThan(8L)) {
        Attempt.failure(Err.InsufficientBits(8L, buffer.size, Nil))
      } else {
        val nextByte = buffer.take(8L).toByte(false)
        val nextValue = value | (nextByte & RelevantDataBits) << shift
        runDecodingBE(buffer.drop(8L), nextByte, nextValue, shift + BitsPerByte)
      }
    } else {
      Attempt.successful(DecodeResult(value, buffer))
    }

  private val RelevantDataBits = 0x7FL
  private val MoreBytesMask = ~RelevantDataBits.toInt
  private val MostSignificantBit = 0x80
  private val BitsPerByte = 7
}
