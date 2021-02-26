package vulcan.binary.internal

import scodec.bits.BitVector
import scodec.{SizeBound, Codec => Scodec}

object ZigZagVarIntCodec extends Scodec[Int] {
  private val toPositiveLong = (i: Int) => {
    (i.toLong << 1) ^ (i.toLong >> 31)
  }

  private val fromPositiveLong = (l: Long) => {
    val i = l.toInt
    (i >>> 1) ^ -(i & 1)
  }

  private[this] val long =
    VarLongCodec
      .xmap(fromPositiveLong, toPositiveLong)

  override def sizeBound =
    SizeBound.bounded(1L, 5L)

  override def encode(i: Int) =
    long.encode(i)

  override def decode(buffer: BitVector) =
    long.decode(buffer)

  override def toString = "variable-length integer"

}
