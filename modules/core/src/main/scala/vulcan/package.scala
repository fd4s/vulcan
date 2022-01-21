package object vulcan {
  type AvroBoolean = Boolean
  type AvroInt = Int
  type AvroLong = Long
  type AvroFloat = Float
  type AvroDouble = Double
  type AvroString = org.apache.avro.util.Utf8
  type AvroBytes = java.nio.ByteBuffer
  type AvroFixed = org.apache.avro.generic.GenericFixed
  type AvroRecord = org.apache.avro.generic.GenericRecord
  type AvroArray[A] = java.util.List[A]
}
