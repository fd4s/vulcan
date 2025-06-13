---
id: overview
title: Overview
---

Vulcan provides Avro schemas, encoders, and decoders between Scala types and types used by the official Apache Avro library. The aims are reduced boilerplate code and improved type safety, compared to when directly using the Apache Avro library. In particular, the following features are supported.

- Schemas, encoders, and decoders for many standard library types.

- Ability to easily create schemas, encoders, and decoders for custom types.

- Derivation of schemas, encoders, and decoders for `case class`es and `sealed trait`s.

Documentation is kept up-to-date, currently documenting v@LATEST_VERSION@ on Scala @DOCS_SCALA_VERSION@.

## Getting Started

To get started with [sbt](https://scala-sbt.org), simply add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@LATEST_VERSION@"
```

Published for Scala @SCALA_PUBLISH_VERSIONS@. For changes, refer to the [release notes](https://github.com/fd4s/vulcan/releases).

For Scala 2.12, enable partial unification by adding the following line to `build.sbt`.

```scala
scalacOptions += "-Ypartial-unification"
```

### Modules

Following are additional provided modules.

#### Enumeratum

For [enumeratum](modules.md#enumeratum) support, add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@ENUMERATUM_MODULE_NAME@" % "@LATEST_VERSION@"
```

#### Generic

For [generic derivation](modules.md#generic) support, add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@GENERIC_MODULE_NAME@" % "@LATEST_VERSION@"
```

#### Refined

For [refined](modules.md#refined) support, add the following line to your `build.sbt` file.

```scala
libraryDependencies += "@ORGANIZATION@" %% "@REFINED_MODULE_NAME@" % "@LATEST_VERSION@"
```

#### External Modules

Following is an incomplete list of third-party integrations.

- [fs2-kafka-vulcan](https://fd4s.github.io/fs2-kafka)

### Signatures

Stable release artifacts are signed with the [`7AD5 92B5 B105 24E3`](https://keys.openpgp.org/search?q=4EA20EFC74E422D0489470997AD592B5B10524E3) key.

### Compatibility

Backwards binary-compatibility for the library is guaranteed between minor and patch versions.<br>
Version `@LATEST_MAJOR_VERSION@.a.b` is backwards binary-compatible with `@LATEST_MAJOR_VERSION@.c.d` for any `a > c` or `a = c` and `b > d`.

Please note binary-compatibility is not guaranteed between milestone releases.

### Snapshot Releases

To use the latest snapshot release, add the following lines to your `build.sbt` file.

```scala
resolvers += Resolver.sonatypeCentralSnapshots

libraryDependencies += "@ORGANIZATION@" %% "@CORE_MODULE_NAME@" % "@LATEST_SNAPSHOT_VERSION@"
```

## Dependencies

Refer to the table below for dependencies and version support across modules.

| Module                     | Dependencies                                                                                                                                    | Scala                                   |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `@CORE_MODULE_NAME@`       | [Apache Avro @AVRO_VERSION@](https://github.com/apache/avro), [Cats @CATS_VERSION@](https://github.com/typelevel/cats)                          | Scala @CORE_CROSS_SCALA_VERSIONS@       |
| `@ENUMERATUM_MODULE_NAME@` | [Enumeratum @ENUMERATUM_VERSION@](https://github.com/lloydmeta/enumeratum)                                                                      | Scala @ENUMERATUM_CROSS_SCALA_VERSIONS@ |
| `@GENERIC_MODULE_NAME@`    | [Magnolia @MAGNOLIA_VERSION@](https://github.com/propensive/magnolia), [Shapeless @SHAPELESS_VERSION@](https://github.com/milessabin/shapeless) | Scala @GENERIC_CROSS_SCALA_VERSIONS@    |
| `@REFINED_MODULE_NAME@`    | [Refined @REFINED_VERSION@](https://github.com/fthomas/refined)                                                                                 | Scala @REFINED_CROSS_SCALA_VERSIONS@    |

## Inspiration

Library is heavily inspired by ideas from [avro4s](https://github.com/sksamuel/avro4s).

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html). Refer to the [license file](https://github.com/fd4s/vulcan/blob/master/license.txt).
