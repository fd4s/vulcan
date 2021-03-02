package vulcan

import cats.syntax.all._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import scodec.bits.ByteVector
import scodec.interop.cats._
import scodec.{Attempt, DecodeResult, Decoder, Encoder, Err, codecs, Codec => Scodec}
import vulcan.binary.internal.{VarLongCodec, ZigZagVarIntCodec}
import vulcan.internal.converters.collection._

import java.nio.ByteBuffer
import scala.reflect.ClassTag

package object binary {

  private[binary] val zigZagVarLong: Scodec[Long] = VarLongCodec.xmap(
    zz => (zz >>> 1L) ^ -(zz & 1L),
    i => (i << 1L) ^ (i >> 63L)
  )

  lazy val intScodec: Scodec[java.lang.Integer] =
    ZigZagVarIntCodec.xmap(java.lang.Integer.valueOf(_), _.intValue)

  val longScodec: Scodec[java.lang.Long] =
    zigZagVarLong.xmap(java.lang.Long.valueOf(_), _.longValue)

  val floatScodec: Scodec[java.lang.Float] =
    codecs.floatL.xmap(java.lang.Float.valueOf(_), _.floatValue)

  val doubleScodec: Scodec[java.lang.Double] =
    codecs.doubleL.xmap(java.lang.Double.valueOf(_), _.doubleValue)

  val stringScodec: Scodec[Utf8] =
    codecs
      .variableSizeBytesLong(zigZagVarLong, codecs.bytes)
      .xmap(bytes => new Utf8(bytes.toArray), utf8 => ByteVector(utf8.getBytes))

  val boolScodec: Scodec[java.lang.Boolean] =
    codecs.byte
      .xmap(b => java.lang.Boolean.valueOf(b == 1), b => if (b.booleanValue) 1: Byte else 0: Byte)

  def enumScodec(writerSchema: Schema): Scodec[GenericData.EnumSymbol] =
    ZigZagVarIntCodec.xmap(
      i => new GenericData.EnumSymbol(writerSchema, writerSchema.getEnumSymbols.get(i)),
      symbol => writerSchema.getEnumOrdinal(symbol.toString)
    )

  def recordEncoder(writerSchema: Schema): Encoder[GenericRecord] =
    Encoder { record =>
      writerSchema.getFields.asScala.toList.zipWithIndex
        .map { case (field, idx) => (forWriterSchema(field.schema), record.get(idx)) }
        .traverse {
          case (codec, value) =>
            codec.encode(value)
        }
        .map(_.reduce(_ ++ _))
    }

  def recordDecoder(writerSchema: Schema): Decoder[GenericRecord] =
    Decoder { bytes =>
      writerSchema.getFields.asScala.toList
        .map(field => forWriterSchema(field.schema).map((field.name, _)))
        .scanLeft[Attempt[DecodeResult[(String, Any)]]](
          Attempt.successful(DecodeResult((null, null), bytes))
        ) { (prev, codec) =>
          prev
            .map(_.remainder)
            .flatMap(codec.decode)
        }
        .sequence
        .map { l =>
          val record = new GenericData.Record(writerSchema)
          l.tail.foreach {
            case DecodeResult((name, value), _) => record.put(name, value)
          }
          DecodeResult(record, l.last.remainder)
        }
    }

  val nullScodec: Scodec[Null] =
    codecs.ignore(0).xmap(_ => null, _ => ())

  def blockScodec[A](codec: Scodec[A]): Scodec[List[A]] =
    codecs.listOfN(ZigZagVarIntCodec, codec)

  private def widenToAny[A](codec: Scodec[A])(implicit ct: ClassTag[A]): Scodec[Any] =
    codec.widen[Any](
      identity, {
        case a if ct.runtimeClass.isInstance(a) => Attempt.successful(a.asInstanceOf[A])
        case other                              => Attempt.failure(Err(s"$other is not a ${ct.runtimeClass.getName}"))
      }
    )

  def forWriterSchema(schema: Schema): Scodec[Any] = schema.getType match {
    case Schema.Type.FLOAT  => widenToAny(floatScodec)
    case Schema.Type.DOUBLE => widenToAny(doubleScodec)
    case Schema.Type.INT    => widenToAny(intScodec)
    case Schema.Type.LONG   => widenToAny(longScodec)
    case Schema.Type.RECORD =>
      widenToAny(Scodec[GenericRecord](recordEncoder(schema), recordDecoder(schema)))
    case Schema.Type.ARRAY =>
      widenToAny(ArrayScodec(forWriterSchema(schema.getElementType)))
    case Schema.Type.STRING  => widenToAny(stringScodec)
    case Schema.Type.BOOLEAN => widenToAny(boolScodec)
    case Schema.Type.NULL =>
      nullScodec.widen[Any](identity, { n =>
        if (n == null) Attempt.successful(null) else Attempt.failure(Err(s"$n is not null"))
      })
    case Schema.Type.ENUM => widenToAny(enumScodec(schema))
    case Schema.Type.UNION =>
      val types = schema.getTypes.asScala.toList
      ZigZagVarIntCodec.consume(schemaIdx => forWriterSchema(types(schemaIdx))) { value =>
        val check: Schema => Boolean =
          if (value == null) _.getType == Schema.Type.NULL
          else
            value match {
              case _: java.lang.Float         => _.getType == Schema.Type.FLOAT
              case _: java.lang.Double        => _.getType == Schema.Type.DOUBLE
              case _: java.lang.Integer       => _.getType == Schema.Type.INT
              case _: java.lang.Long          => _.getType == Schema.Type.LONG
              case _: java.lang.Boolean       => _.getType == Schema.Type.BOOLEAN
              case _: Utf8                    => _.getType == Schema.Type.STRING
              case _: java.util.Map[_, _]     => _.getType == Schema.Type.MAP
              case _: java.util.Collection[_] => _.getType == Schema.Type.ARRAY
              case gr: GenericRecord          => _.getFullName == gr.getSchema.getFullName
              case gf: GenericFixed           => _.getFullName == gf.getSchema.getFullName
              case ge: GenericEnumSymbol[_]   => _.getFullName == ge.getSchema.getFullName
              case _ => throw new AssertionError("match should have been exhaustive")
            }
        types.indexWhere(check)
      }
    case Schema.Type.BYTES =>
      widenToAny {
        codecs.bytes.xmap[ByteBuffer](_.toByteBuffer, ByteVector.apply)
      }
    case Schema.Type.FIXED =>
      widenToAny {
        codecs.fixedSizeBytes(
          schema.getFixedSize.toLong,
          codecs.bytes.xmap[ByteBuffer](_.toByteBuffer, ByteVector.apply)
        )
      }
    case Schema.Type.MAP => ???
  }

}
