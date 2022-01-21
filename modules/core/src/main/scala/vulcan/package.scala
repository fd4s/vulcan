package object vulcan {
  type AvroBoolean = Boolean
  type AvroInt = Int
  type AvroLong = Long
  type AvroFloat = Float
  type AvroDouble = Double
  type AvroString = org.apache.avro.util.Utf8
  type AvroBytes = java.nio.ByteBuffer
  type AvroNull = Null
  type AvroFixed = org.apache.avro.generic.GenericFixed
  type AvroRecord = org.apache.avro.generic.GenericRecord
  type AvroEnum = org.apache.avro.generic.GenericData.EnumSymbol
  type AvroArray[A] = java.util.List[A]
  type AvroMap[A] = java.util.Map[AvroString, A]
}
