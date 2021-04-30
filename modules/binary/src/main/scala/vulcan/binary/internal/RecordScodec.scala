package vulcan.binary.internal

import cats.syntax.all._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scodec.bits.BitVector
import scodec.interop.cats._
import scodec.{Attempt, DecodeResult, Decoder, Err, Codec => Scodec}
import vulcan.binary.forWriterSchema
import vulcan.internal.converters.collection._

class RecordScodec(writerSchema: Schema) extends Scodec[GenericRecord] {
  lazy val encoderFields: List[(String, Scodec[Any])] =
    writerSchema.getFields.asScala.toList
      .map(field => field.name -> forWriterSchema(field.schema))

  override def encode(record: GenericRecord): Attempt[BitVector] =
    encoderFields
      .traverse {
        case (name, codec) =>
          codec.encode(record.get(name))
      }
      .map(_.reduce(_ ++ _))

  lazy val decoderFields: List[Decoder[(String, Any)]] =
    writerSchema.getFields.asScala.toList
      .map(field => forWriterSchema(field.schema).map((field.name, _)).asDecoder)

  override def decode(bits: BitVector): Attempt[DecodeResult[GenericRecord]] =
    RecordScodec.decodeFields(decoderFields).decode(bits).map {
      _.map { fields =>
        val record = new GenericData.Record(writerSchema)
        fields.foreach {
          case (name, value) => record.put(name, value)
        }
        record
      }
    }
  override lazy val sizeBound = encoderFields.map(_._2.sizeBound).reduce(_ + _)
}

object RecordScodec {
  private def decodeFields(
    decoderFields: List[Decoder[(String, Any)]]
  ): Decoder[List[(String, Any)]] = Decoder { bits =>
    decoderFields
      .scanLeft[Attempt[DecodeResult[(String, Any)]]](
        Attempt.successful(DecodeResult((null, null), bits))
      ) { (prev, codec) =>
        prev
          .map(_.remainder)
          .flatMap(codec.decode)
      }
      .sequence
      .map { l =>
        val value = l.tail.map(_.value)
        DecodeResult(value, l.last.remainder)
      }
  }

  def resolve(writerSchema: Schema, readerSchema: Schema): Decoder[GenericRecord] = {
    val writerFields = writerSchema.getFields.asScala.toList
    val readerFields = readerSchema.getFields.asScala.toList
    val readerMap = readerFields.map { field =>
      field.name -> field
    }.toMap
    val resolvedWriterFields: Attempt[List[Decoder[(String, Any)]]] = writerFields.traverse {
      field =>
        (readerMap.get(field.name) match {
          case None => Attempt.successful(forWriterSchema(field.schema))
          case Some(readerField) =>
            Attempt.fromOption(
              vulcan.binary.resolve(field.schema(), readerField.schema),
              Err(s"incompatible schemas for field ${field.name}")
            )
        }).map(_.tupleLeft(field.name))
    }

    Decoder.liftAttempt(resolvedWriterFields).flatMap(decodeFields).flatMap {
      resolvedDecodedFields =>
        val resolvedDecodedFieldsMap = resolvedDecodedFields.toMap

        val record = new GenericData.Record(readerSchema)

        Decoder.liftAttempt(
          readerFields
            .traverse { readerField =>
              Attempt.fromOption(
                {
                  val maybeVal = resolvedDecodedFieldsMap
                    .get(readerField.name)
                    .orElse(Option(readerField.defaultVal))
                  maybeVal.foreach(record.put(readerField.name, _))
                  maybeVal
                },
                Err(s"missing field ${readerField.name} with no default")
              )
            }
            .as(record)
        )
    }
  }
}
