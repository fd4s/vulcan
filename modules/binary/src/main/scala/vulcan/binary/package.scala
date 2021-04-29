package vulcan

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.avro.util.Utf8
import cats.syntax.all._
import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, DecodeResult, Decoder, Err, codecs, Codec => Scodec}
import vulcan.binary.internal._
import vulcan.internal.converters.collection._

import java.nio.ByteBuffer
import scala.reflect.ClassTag

package object binary {

  lazy val intScodec: Scodec[Int] = ZigZagVarIntCodec

  val longScodec: Scodec[Long] = VarLongCodec.xmap(
    zz => (zz >>> 1L) ^ -(zz & 1L),
    i => (i << 1L) ^ (i >> 63L)
  )

  val floatScodec: Scodec[Float] = codecs.floatL

  val doubleScodec: Scodec[Double] = codecs.doubleL

  val stringScodec: Scodec[Utf8] =
    codecs
      .variableSizeBytesLong(longScodec, codecs.bytes)
      .xmap(bytes => new Utf8(bytes.toArray), utf8 => ByteVector(utf8.getBytes))

  val boolScodec: Scodec[Boolean] =
    codecs.byte
      .xmap(_ == 1, b => if (b) 1: Byte else 0: Byte)

  val bytesScodec: Scodec[ByteBuffer] =
    codecs
      .variableSizeBytesLong(longScodec, codecs.bytes)
      .xmap[ByteBuffer](_.toByteBuffer, ByteVector.apply)

  def fixedScodec(schema: Schema): Scodec[GenericFixed] = {
    require(schema.getType == FIXED)
    codecs.fixedSizeBytes(
      schema.getFixedSize.toLong,
      codecs.bytes.xmapc { b =>
        new GenericData.Fixed(schema, b.toArray): GenericFixed
      }(gf => ByteVector(gf.bytes))
    )
  }

  def enumScodec(writerSchema: Schema): Scodec[GenericData.EnumSymbol] =
    ZigZagVarIntCodec.xmap(
      i => new GenericData.EnumSymbol(writerSchema, writerSchema.getEnumSymbols.get(i)),
      symbol => writerSchema.getEnumOrdinal(symbol.toString)
    )

  def enumResolver(writerSchema: Schema, readerSchema: Schema): Decoder[GenericData.EnumSymbol] =
    ZigZagVarIntCodec.emap { writerIdx =>
      val symbol = writerSchema.getEnumSymbols.get(writerIdx)
      if (readerSchema.hasEnumSymbol(symbol))
        Attempt.successful(new GenericData.EnumSymbol(readerSchema, symbol))
      else
        readerSchema.getEnumDefault match {
          case null =>
            Attempt.failure(
              Err(
                s"Symbol $symbol is not present in ${readerSchema.getName} and there is no default"
              )
            )
          case default => Attempt.successful(new GenericData.EnumSymbol(readerSchema, default))
        }
    }

  val nullScodec: Scodec[Null] =
    codecs.ignore(0).xmap(_ => null, _ => ())

  private def widenToAny[A](codec: Scodec[A])(implicit ct: ClassTag[A]): Scodec[Any] =
    codec.widen[Any](
      identity, {
        case a: A  => Attempt.successful(a)
        case other => Attempt.failure(Err(s"$other is not a ${ct.runtimeClass.getName}"))
      }
    )

  def forWriterSchema(schema: Schema): Scodec[Any] = schema.getType match {
    case FLOAT  => widenToAny(floatScodec)
    case DOUBLE => widenToAny(doubleScodec)
    case INT    => widenToAny(intScodec)
    case LONG   => widenToAny(longScodec)
    case RECORD =>
      widenToAny(new RecordScodec(schema))
    case ARRAY =>
      widenToAny(ArrayScodec(forWriterSchema(schema.getElementType)))
    case STRING  => widenToAny(stringScodec)
    case BOOLEAN => widenToAny(boolScodec)
    case NULL =>
      nullScodec.widen[Any](identity, { n =>
        if (n == null) Attempt.successful(null) else Attempt.failure(Err(s"$n is not null"))
      })
    case ENUM => widenToAny(enumScodec(schema))
    case UNION =>
      val types = schema.getTypes.asScala.toList
      intScodec.consume(schemaIdx => forWriterSchema(types(schemaIdx))) { value =>
        val check: Schema => Boolean =
          value match {
            case null                       => _.getType == NULL
            case _: Float                   => _.getType == FLOAT
            case _: Double                  => _.getType == DOUBLE
            case _: Int                     => _.getType == INT
            case _: Long                    => _.getType == LONG
            case _: Boolean                 => _.getType == BOOLEAN
            case _: Utf8                    => _.getType == STRING
            case _: java.util.Map[_, _]     => _.getType == MAP
            case _: java.util.Collection[_] => _.getType == ARRAY
            case gr: GenericRecord          => _.getFullName == gr.getSchema.getFullName
            case gf: GenericFixed           => _.getFullName == gf.getSchema.getFullName
            case ge: GenericEnumSymbol[_]   => _.getFullName == ge.getSchema.getFullName
            case _                          => throw new AssertionError("match should have been exhaustive")
          }
        types.indexWhere(check)
      }
    case BYTES => widenToAny(bytesScodec)
    case FIXED => widenToAny(fixedScodec(schema))
    case MAP   => widenToAny(new MapScodec(forWriterSchema(schema.getValueType)))
  }

  // WIP
  def resolve(writerSchema: Schema, readerSchema: Schema): Option[Decoder[Any]] =
    (readerSchema.getType, writerSchema.getType) match {
      case (FLOAT, FLOAT)   => floatScodec.some
      case (FLOAT, INT)     => intScodec.map(_.toFloat).some
      case (DOUBLE, DOUBLE) => doubleScodec.some
      case (DOUBLE, FLOAT)  => floatScodec.map(_.toDouble).some
      case (DOUBLE, INT)    => intScodec.map(_.toDouble).some
      case (DOUBLE, LONG)   => longScodec.map(_.toDouble).some
      case (INT, INT)       => intScodec.some
      case (LONG, LONG)     => longScodec.some
      case (LONG, INT)      => intScodec.map(_.toLong).some
      case (RECORD, RECORD) if readerSchema.getName == writerSchema.getName =>
        RecordScodec.resolve(writerSchema, readerSchema).some
      case (ARRAY, ARRAY) =>
        ArrayScodec[Any](
          Scodec[Any](
            codecs.fail[Any](Err("decode only")),
            resolve(writerSchema.getElementType, readerSchema.getElementType).getOrElse(
              mismatch(writerSchema.getElementType.getType, readerSchema.getElementType.getType)
            )
          )
        ).some
      case (STRING, STRING | BYTES) => stringScodec.some

      case (BOOLEAN, BOOLEAN) => boolScodec.some
      case (NULL, NULL) =>
        nullScodec
          .widen[Any](identity, { n =>
            if (n == null) Attempt.successful(null) else Attempt.failure(Err(s"$n is not null"))
          })
          .some
      case (ENUM, ENUM) if readerSchema.getName == writerSchema.getName =>
        enumResolver(writerSchema, readerSchema).some
      case (UNION, _) =>
        val types = writerSchema.getTypes.asScala.toList
        intScodec
          .flatMap(
            writerSchemaIdx =>
              resolve(types(writerSchemaIdx), readerSchema)
                .getOrElse(codecs.fail[Any](Err("exhausted alternatives for union")))
          )
          .some
      case (_, UNION) =>
        readerSchema.getTypes.asScala.toList.collectFirstSome(resolve(writerSchema, _))
      case (BYTES, BYTES | STRING) =>
        bytesScodec.some
      case (FIXED, FIXED)
          if readerSchema.getName == writerSchema.getName && readerSchema.getFixedSize == writerSchema.getFixedSize =>
        fixedScodec(readerSchema).some
      case (MAP, MAP) =>
        resolve(writerSchema.getValueType, readerSchema.getValueType).map { resolved =>
          new MapScodec(
            Scodec(
              codecs.fail[Any](Err("decode only")),
              resolved
            )
          )
        }
    }

  def mismatch(readerType: Schema.Type, writerType: Schema.Type): Decoder[Any] =
    codecs.fail[Any](
      Err(
        s"Reader schema of type $readerType cannot read output of writer schema of type $writerType"
      )
    )

  def encode[A](a: A)(implicit codec: Codec[A]): Either[AvroError, Array[Byte]] =
    codec.schema.flatMap { schema =>
      codec.encode(a).flatMap { jAvro =>
        forWriterSchema(schema).encode(jAvro) match {
          case Attempt.Successful(bits) => Right(bits.toByteArray)
          case Attempt.Failure(cause)   => Left(AvroError(cause.messageWithContext))
        }
      }
    }

  def decode[A](bytes: Array[Byte], writerSchema: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    codec.schema.flatMap { readerSchema =>
      resolve(readerSchema, writerSchema)
        .getOrElse(mismatch(readerSchema.getType, writerSchema.getType))
        .decode(BitVector(bytes)) match {
        case Attempt.Successful(DecodeResult(value, _)) => codec.decode(value, writerSchema)
        case Attempt.Failure(cause)                     => Left(AvroError(cause.messageWithContext))
      }
    }

}
