package vulcan.generic

final case class Configuration(typeSeparators: Option[(String, String)]) {
  def withTypeSeparators(genericTypeSep: String, typeSep: String): Configuration =
    copy(typeSeparators = Some((genericTypeSep, typeSep)))
}

object Configuration {
  val default = Configuration(None)
}

object defaults {
  implicit val defaultGenericConfiguration: Configuration = Configuration.default
}

object avro4s {
  implicit val avro4sGenericConfiguration: Configuration =
    Configuration.default.withTypeSeparators("__", "_")
}