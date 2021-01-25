package vulcan

import cats.syntax.all._
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericFixed, GenericEnumSymbol, GenericRecord, GenericData}

import vulcan.internal.converters.collection._

sealed trait Avro

object Avro {
  final case class Boolean(value: scala.Boolean) extends Avro
  final case class Int(value: scala.Int) extends Avro
  final case class Long(value: scala.Long) extends Avro
  final case class Bytes(value: ByteBuffer) extends Avro
  final case class Fixed(value: scala.Array[Byte]) extends Avro
  final case class String(value: java.lang.String) extends Avro
  final case class Float(value: scala.Float) extends Avro
  final case class Double(value: scala.Double) extends Avro
  final case class Array(values: Vector[Avro]) extends Avro
  final case class Enum(value: java.lang.String) extends Avro
  final case class Record(values: scala.collection.Map[java.lang.String, Avro]) extends Avro
  final case class Map(values: scala.collection.Map[java.lang.String, Avro]) extends Avro

  def fromJava(jAvro: Any): Either[AvroError, Avro] = jAvro match {
    case boolean: java.lang.Boolean => Boolean(boolean).asRight
    case int: java.lang.Integer     => Int(int).asRight
    case long: java.lang.Long       => Long(long).asRight
    case float: java.lang.Float     => Float(float).asRight
    case double: java.lang.Double   => Double(double).asRight
    case s: java.lang.String        => String(s).asRight
    case utf8: Utf8                 => String(utf8.toString()).asRight
    case bytes: ByteBuffer          => Bytes(bytes).asRight
    case fixed: GenericFixed        => Fixed(fixed.bytes()).asRight
    case collection: java.util.Collection[_] =>
      collection.asScala.toVector.traverse(fromJava).map(Array.apply)
    case genericEnum: GenericEnumSymbol[_] => Enum(genericEnum.toString()).asRight
    case record: GenericRecord =>
      record.getSchema.getFields.asScala.toList
        .traverse { field =>
          fromJava(record.get(field.name())).tupleLeft(field.name())
        }
        .map(fields => Record(fields.toMap))
    case map: java.util.Map[_, _] =>
      map.asScala.toList
        .traverse {
          case (key: Utf8, value) =>
            fromJava(value).tupleLeft(key.toString)
          case (key, _) =>
            Left(AvroError.decodeUnexpectedMapKey(key))
        }
        .map(kvs => Map(kvs.toMap))
  }

  def toJava(avro: Avro, schema: Schema): Either[AvroError, Any] = avro match {
    case Boolean(value) => java.lang.Boolean.valueOf(value).asRight
    case Int(value)     => java.lang.Integer.valueOf(value).asRight
    case Long(value)    => java.lang.Long.valueOf(value).asRight
    case Bytes(value)   => value.asRight
    case Fixed(bytes) =>
      AvroError.catchNonFatal {
        val size = schema.getFixedSize()
        if (bytes.length <= size) {
          val buffer = ByteBuffer.allocate(size).put(bytes)
          GenericData.get().createFixed(null, buffer.array(), schema).asRight
        } else {
          Left(AvroError.encodeExceedsFixedSize(bytes.length, size, "foo"))
        }
      }
    case String(value)  => ???
    case Float(value)   => ???
    case Double(value)  => ???
    case Array(values)  => ???
    case Enum(value)    => ???
    case Record(values) => ???
    case Map(values)    => ???
  }
}
