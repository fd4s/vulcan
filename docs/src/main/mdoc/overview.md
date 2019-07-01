---
id: overview
title: Overview
---

Vulcan provides Avro schemas, encoders, and decoders between Scala types and types used by the official Apache Avro library. The aims are reduced boilerplate code and improved type safety, compared to when directly using the Apache Avro library. In particular, the following features are supported.

- Schemas, encoders, and decoders for many standard library types.

- Ability to easily create schemas, encoders, and decoders for custom types.

- Derivation of schemas, encoders, and decoders for `case class`es and `sealed trait`s.

Documentation is kept up-to-date with new releases, currently documenting v@LATEST_VERSION@ on Scala @DOCS_SCALA_MINOR_VERSION@.

## Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following lines to your `build.sbt` file.

```scala
libraryDependencies ++= Seq(
  "@ORGANIZATION@" %% "@CORE_MODULE_NAME@",
  "@ORGANIZATION@" %% "@REFINED_MODULE_NAME@"
).map(_ % "@LATEST_VERSION@")
```

Published for Scala @SCALA_PUBLISH_VERSIONS@. For changes, refer to the [release notes](https://github.com/ovotech/vulcan/releases).

Remember to enable partial unification by adding the following line to `build.sbt`.

```scala
scalacOptions += "-Ypartial-unification"
```

### Compatibility

Backwards binary-compatibility for the library is guaranteed between patch versions.<br>
For example, `@LATEST_MINOR_VERSION@.x` is backwards binary-compatible with `@LATEST_MINOR_VERSION@.y` for any `x > y`.

## Dependencies

The `vulcan` module has the following dependencies.

- Apache Avro @AVRO_VERSION@ ([Documentation](https://avro.apache.org/docs/@AVRO_VERSION@), [GitHub](https://github.com/apache/avro)).
- Magnolia @MAGNOLIA_VERSION@ ([Documentation](https://propensive.com/opensource/magnolia/), [GitHub](https://github.com/propensive/magnolia)).
- Cats @CATS_VERSION@ ([Documentation](https://typelevel.org/cats), [GitHub](https://github.com/typelevel/cats)).
- Shapeless @SHAPELESS_VERSION@ ([GitHub](https://github.com/milessabin/shapeless)).

Additional modules have the following dependencies.

- `@REFINED_MODULE_NAME@` depends on refined @REFINED_VERSION@ ([GitHub](https://github.com/fthomas/refined)).

## Inspiration

Library is heavily inspired by ideas from [avro4s](https://github.com/sksamuel/avro4s).

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/ovotech/vulcan/blob/master/license.txt).
