package vulcan

import cats.syntax.all._
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericFixed, GenericRecord}

import vulcan.internal.converters.collection._
import org.apache.avro.generic.GenericEnumSymbol
import java.nio.charset.StandardCharsets
import org.apache.avro.LogicalType

sealed trait Avro

object Avro {
  final case class Boolean(value: scala.Boolean) extends Avro
  final case class Int(value: scala.Int, logicalType: Option[LogicalType]) extends Avro
  final case class Long(value: scala.Long, logicalType: Option[LogicalType]) extends Avro
  final case class Bytes(value: ByteBuffer, logicalType: Option[LogicalType]) extends Avro
  final case class Fixed(value: scala.Array[Byte], schema: Schema) extends Avro {
    require(schema.getType() == Schema.Type.FIXED)

    val fixedSize: scala.Int = schema.getFixedSize()

    require(value.lengthCompare(fixedSize) == 0)
  }
  final case class String(value: java.lang.String, logicalType: Option[LogicalType]) extends Avro
  final case class Float(value: scala.Float) extends Avro
  final case class Double(value: scala.Double) extends Avro
  final case class Array[Elem <: Avro](values: Vector[Elem]) extends Avro
  final case class Enum(value: java.lang.String, schema: Schema) extends Avro
  final case class Record(
    fields: scala.collection.immutable.Map[java.lang.String, Avro],
    schema: Schema
  ) extends Avro
  final case class Map[Elem <: Avro](values: scala.collection.immutable.Map[java.lang.String, Elem])
      extends Avro
  case object Null extends Avro
  type Null = Null.type

  def fromJava(jAvro: Any, schema: Schema): Either[AvroError, Avro] =
    (schema.getType(), jAvro) match {
      //case (schema, a: Avro) => throw new IllegalArgumentException(s"expected Java $schema, got $a")
      //case (schema, ())      => throw new IllegalArgumentException(s"expected Java $schema, got ()")
      case (Schema.Type.RECORD, record: GenericRecord) =>
        schema
          .getFields()
          .asScala
          .toList
          .traverse { field =>
            AvroError.catchNonFatal {
              (if (record.hasField(field.name)) {
                 fromJava(record.get(field.name()), field.schema)
               } else {
                 fromJava(field.defaultVal(), field.schema)
               }).tupleLeft(field.name())
            }
          }
          .map(fields => Record(fields.toMap, schema))
      case (Schema.Type.ENUM, genericEnum: GenericEnumSymbol[_]) =>
        Right(Enum(genericEnum.toString, schema))
      case (Schema.Type.ARRAY, collection: java.util.Collection[_]) => {
        val element = schema.getElementType
        collection.asScala.toVector.traverse(fromJava(_, element)).map(Array(_))
      }
      case (Schema.Type.MAP, map: java.util.Map[_, _]) =>
        val valueSchema = schema.getValueType()
        map.asScala.toList
          .traverse {
            case (key: Utf8, value) =>
              fromJava(value, valueSchema).tupleLeft(key.toString)
            case (key, _) =>
              Left(AvroError.decodeUnexpectedMapKey(key))
          }
          .map(kvs => Map(kvs.toMap))

      case (Schema.Type.UNION, value) =>
        schema.getTypes.asScala.toList
          .collectFirstSome { altSchema =>
            fromJava(value, altSchema).toOption
          }
          .toRight {
            AvroError(s"Exhausted alternatives for $value - schema was $schema")
            //AvroError.decodeExhaustedAlternatives(value, None)
          }

      case (Schema.Type.FIXED, fixed: GenericFixed) =>
        val bytes = fixed.bytes()
        if (bytes.length == schema.getFixedSize()) {
          Fixed(bytes, schema).asRight
        } else {
          Left {
            AvroError.decodeNotEqualFixedSize(
              bytes.length,
              schema.getFixedSize(),
              "Fixed"
            )
          }
        }
      case (Schema.Type.STRING, string: java.lang.String) =>
        String(string, Option(schema.getLogicalType())).asRight
      case (Schema.Type.STRING, utf8: Utf8) =>
        String(utf8.toString, Option(schema.getLogicalType())).asRight
      case (Schema.Type.BYTES, bytes: ByteBuffer) =>
        Bytes(bytes, Option(schema.getLogicalType())).asRight
      case (Schema.Type.BYTES, string: java.lang.String) =>
        Bytes(
          ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8)),
          Option(schema.getLogicalType())
        ).asRight
      case (Schema.Type.BYTES, utf8: Utf8) =>
        Bytes(ByteBuffer.wrap(utf8.getBytes()), Option(schema.getLogicalType())).asRight
      case (Schema.Type.INT, int: java.lang.Integer) =>
        Int(int, Option(schema.getLogicalType())).asRight
      case (Schema.Type.LONG, long: java.lang.Long) =>
        Long(long, Option(schema.getLogicalType())).asRight
      case (Schema.Type.FLOAT, float: java.lang.Float)       => Float(float).asRight
      case (Schema.Type.DOUBLE, double: java.lang.Double)    => Double(double).asRight
      case (Schema.Type.BOOLEAN, boolean: java.lang.Boolean) => Boolean(boolean).asRight
      case (Schema.Type.NULL, null)                          => Null.asRight
      case (schemaType, value) =>
        AvroError(
          s"Unexpected value converting schema type $schemaType from Java - found $value of type ${value.getClass.getName}"
        ).asLeft
    }

  def toJava(avro: Avro): Either[AvroError, Any] = avro match {
    case Boolean(value)  => java.lang.Boolean.valueOf(value).asRight
    case Int(value, _)   => java.lang.Integer.valueOf(value).asRight
    case Long(value, _)  => java.lang.Long.valueOf(value).asRight
    case Bytes(value, _) => value.asRight
    case fixed @ Fixed(bytes, schema) => {
      val buffer = ByteBuffer.allocate(fixed.fixedSize).put(bytes)
      GenericData.get().createFixed(null, buffer.array(), schema).asRight
    }
    case String(s, _)        => new Utf8(s).asRight
    case Float(f)            => java.lang.Float.valueOf(f).asRight
    case Double(d)           => java.lang.Double.valueOf(d).asRight
    case Array(items)        => items.traverse(toJava).map(_.asJava)
    case Enum(value, schema) => GenericData.get().createEnum(value, schema).asRight
    case Record(fields, schema) =>
      val encodedFields = fields.toList.traverse {
        case (name, value) => toJava(value).tupleLeft(name)
      }

      encodedFields.map { fields =>
        val record = new GenericData.Record(schema)
        fields.foreach {
          case (name, value) => record.put(name, value)
        }
        record
      }
    case Map(values) =>
      values.toList.traverse { case (k, v) => toJava(v).tupleLeft(new Utf8(k)) }.map(_.toMap.asJava)
    case Null => Right(null)
  }
}
