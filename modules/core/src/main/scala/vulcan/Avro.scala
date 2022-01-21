package vulcan

/*
 * Types modelling the Avro format. Because we use the Apache Avro Java library for serialization to
 * bytes or JSON, these are the same types as those used by that library.
 */
object Avro {
  type Boolean = scala.Boolean
  type Int = scala.Int
  type Long = scala.Long
  type Float = scala.Float
  type Double = scala.Double
  type String = org.apache.avro.util.Utf8
  type Bytes = java.nio.ByteBuffer
  type Null = scala.Null
  type Fixed = org.apache.avro.generic.GenericFixed
  type Record = org.apache.avro.generic.GenericRecord
  type EnumSymbol = org.apache.avro.generic.GenericData.EnumSymbol
  type Array[A] = java.util.List[A]
  type Map[A] = java.util.Map[String, A]
}
