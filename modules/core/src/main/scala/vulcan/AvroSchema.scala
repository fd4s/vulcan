// package vulcan

// import cats.syntax.all._
// import org.apache.avro.{Schema, SchemaBuilder}
// import vulcan.internal.converters.collection._

// sealed trait AvroSchema {
//   def toJava: Either[AvroError, Schema] = this match {
//     case AvroSchema.Boolean       => SchemaBuilder.builder().booleanType().asRight
//     case AvroSchema.Int           => SchemaBuilder.builder().intType().asRight
//     case AvroSchema.Long          => SchemaBuilder.builder().longType().asRight
//     case AvroSchema.Bytes         => SchemaBuilder.builder().bytesType().asRight
//     case AvroSchema.String        => SchemaBuilder.builder().stringType().asRight
//     case AvroSchema.Float         => SchemaBuilder.builder().floatType().asRight
//     case AvroSchema.Double        => SchemaBuilder.builder().doubleType().asRight
//     case AvroSchema.Array(values) => values.toJava.map(Schema.createArray)
//     case AvroSchema.Enumeration(name, namespace, symbols, default, doc, _) =>
//       AvroError.catchNonFatal(
//         Schema
//           .createEnum(
//             name,
//             doc.orNull,
//             namespace,
//             symbols.asJava,
//             default.orNull
//           )
//           .asRight
//       )
//     case AvroSchema.Fixed(name, namespace, size, doc, aliases) =>
//       AvroError.catchNonFatal(
//         SchemaFactory.fixed(name, namespace, aliases.toArray, doc.orNull, size).asRight
//       )
//     case null => ???
//   }
// }

// object AvroSchema {
//   case object Boolean extends AvroSchema
//   case object Int extends AvroSchema
//   case object Long extends AvroSchema
//   case object Bytes extends AvroSchema
//   case object String extends AvroSchema
//   case object Float extends AvroSchema
//   case object Double extends AvroSchema

//   final case class Array(values: AvroSchema) extends AvroSchema
//   final case class Enumeration(
//     name: String,
//     namespace: String,
//     symbols: Seq[String],
//     default: Option[String],
//     doc: Option[String],
//     aliases: Seq[String]
//   ) extends AvroSchema

//   final case class Fixed(
//     name: String,
//     namespace: String,
//     size: Int,
//     doc: Option[String],
//     aliases: Seq[String]
//   ) extends AvroSchema

//   final case class Record(
//     name: String,
//     namespace: String,
//     fields: Seq[Record.Field],
//     doc: Option[String],
//     aliases: Seq[String]
//   )
//   object Record {
//     final case class Field(name: String, schema: AvroSchema, doc: Option[String])
//   }
//   final case class Union(alternatives: Seq[AvroSchema])
// }
