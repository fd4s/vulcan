package vulcan;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FixedBuilder;

/*
 * This is a temporary workaround for a bug involving Java interop in Scala 3.
 * https://github.com/lampepfl/dotty/issues/9492
 */
class SchemaFactory {
  public static Schema fixed(String name, String namespace, String[] aliases, String doc, int size) {
    return SchemaBuilder
      .builder(namespace)
      .fixed(name)
      .aliases(aliases)
      .doc(doc)
      .size(size);
  }

  public static Schema enumeration(String namespace, String[] symbols) {
    return SchemaBuilder
      .enumeration(namespace)
      .symbols(symbols);
  }
}
