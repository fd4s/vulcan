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
  final case class ABoolean(value: Boolean) extends Avro
  final case class AInt(value: Int, logicalType: Option[LogicalType]) extends Avro
  final case class ALong(value: Long, logicalType: Option[LogicalType]) extends Avro
  final case class ABytes(value: ByteBuffer, logicalType: Option[LogicalType]) extends Avro
  final case class AFixed(value: Array[Byte], schema: Schema) extends Avro {
    require(schema.getType() == Schema.Type.FIXED)

    val fixedSize: Int = schema.getFixedSize()

    require(value.lengthCompare(fixedSize) == 0)
  }
  final case class AString(value: String, logicalType: Option[LogicalType]) extends Avro
  final case class AFloat(value: Float) extends Avro
  final case class ADouble(value: Double) extends Avro
  final case class AArray(values: Vector[Avro]) extends Avro
  final case class AEnum(value: String, schema: Schema) extends Avro
  final case class ARecord(fields: Map[String, Avro], schema: Schema) extends Avro
  final case class AMap(values: Map[String, Avro], valueSchema: Schema) extends Avro
  case object ANull extends Avro

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
          .map(fields => ARecord(fields.toMap, schema))
      case (Schema.Type.ENUM, genericEnum: GenericEnumSymbol[_]) =>
        val symbols = schema.getEnumSymbols().asScala.toList
        val symbol = genericEnum.toString()

        if (symbols.contains(symbol))
          Right(AEnum(symbol, schema))
        else
          Left(AvroError.decodeSymbolNotInSchema(symbol, symbols, "foo"))
      case (Schema.Type.ARRAY, collection: java.util.Collection[_]) => {
        val element = schema.getElementType
        collection.asScala.toVector.traverse(fromJava(_, element)).map(AArray)
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
          .map(kvs => AMap(kvs.toMap, valueSchema))

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
          AFixed(bytes, schema).asRight
        } else {
          Left {
            AvroError.decodeNotEqualFixedSize(
              bytes.length,
              schema.getFixedSize(),
              "AFixed"
            )
          }
        }
      case (Schema.Type.STRING, string: String) =>
        AString(string, Option(schema.getLogicalType())).asRight
      case (Schema.Type.STRING, utf8: Utf8) =>
        AString(utf8.toString, Option(schema.getLogicalType())).asRight
      case (Schema.Type.BYTES, bytes: ByteBuffer) =>
        ABytes(bytes, Option(schema.getLogicalType())).asRight
      case (Schema.Type.BYTES, string: String) =>
        ABytes(
          ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8)),
          Option(schema.getLogicalType())
        ).asRight
      case (Schema.Type.BYTES, utf8: Utf8) =>
        ABytes(ByteBuffer.wrap(utf8.getBytes()), Option(schema.getLogicalType())).asRight
      case (Schema.Type.INT, int: java.lang.Integer) =>
        AInt(int, Option(schema.getLogicalType())).asRight
      case (Schema.Type.LONG, long: java.lang.Long) =>
        ALong(long, Option(schema.getLogicalType())).asRight
      case (Schema.Type.FLOAT, float: java.lang.Float)       => AFloat(float).asRight
      case (Schema.Type.DOUBLE, double: java.lang.Double)    => ADouble(double).asRight
      case (Schema.Type.BOOLEAN, boolean: java.lang.Boolean) => ABoolean(boolean).asRight
      case (Schema.Type.NULL, null)                          => ANull.asRight
      case (schemaType, value) =>
        AvroError(
          s"Unexpected value converting schema type $schemaType from Java - found $value of type ${value.getClass.getName}"
        ).asLeft
    }

  def toJava(avro: Avro): Either[AvroError, Any] = avro match {
    case ABoolean(value)  => java.lang.Boolean.valueOf(value).asRight
    case AInt(value, _)   => java.lang.Integer.valueOf(value).asRight
    case ALong(value, _)  => java.lang.Long.valueOf(value).asRight
    case ABytes(value, _) => value.asRight
    case fixed @ AFixed(bytes, schema) => {
      val buffer = ByteBuffer.allocate(fixed.fixedSize).put(bytes)
      GenericData.get().createFixed(null, buffer.array(), schema).asRight
    }
    case AString(s, _)        => new Utf8(s).asRight
    case AFloat(f)            => java.lang.Float.valueOf(f).asRight
    case ADouble(d)           => java.lang.Double.valueOf(d).asRight
    case AArray(items)        => items.traverse(toJava).map(_.asJava)
    case AEnum(value, schema) => GenericData.get().createEnum(value, schema).asRight
    case ARecord(fields, schema) =>
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
    case AMap(values, _) =>
      values.toList.traverse { case (k, v) => toJava(v).tupleLeft(new Utf8(k)) }.map(_.toMap.asJava)
    case ANull => Right(null)
  }
}
