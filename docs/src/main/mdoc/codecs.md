---
id: codecs
title: Codecs
---

[`Codec`][codec] is the central concept in the library. [`Codec`][codec]s represent an Avro `schema`, together with an `encode` and `decode` function for converting between Scala types and types recognized by the Apache Avro library. There are default [`Codec`][codec]s defined for many standard library types. For example, we can check what the default `Option[Instant]` encoding is by asking for a [`Codec`][codec] instance.

```scala mdoc
import java.time.Instant
import vulcan.Codec

Codec[Option[Instant]]
```

In some cases, it's not possible to generate Avro schemas. This is why [`Codec`][codec] `schema`s are wrapped in `Either` with error type [`AvroError`][avroerror]. For example, Avro doesn't support nested unions, so what would happen when we try to ask for a [`Codec`][codec] for `Option[Option[Instant]]`?

```scala mdoc
Codec[Option[Option[Instant]]]
```

Encoding and decoding with a [`Codec`][codec] might also be unsuccessful, so results are wrapped in `Either` with error type [`AvroError`][avroerror]. Encoding and decoding accepts a value and schema, and attempts encoding or decoding with respect to the schema. What happens if we try to encode `Int`s using a `Boolean` schema?

```scala mdoc
import org.apache.avro.SchemaBuilder

Codec[Int].encode(10, SchemaBuilder.builder.booleanType)
```

Since the Apache Avro library encodes and decodes using `Object`, [`Codec`][codec]s encode and decode between Scala types and `Any`. This means type safety is lost and tests should be used to ensure [`Codec`][codec]s work as intended. This becomes important when we define [`Codec`][codec]s from scratch. Note `Schemas`s are treated as effectively immutable, even though they're in fact mutable.

[`Codec`][codec]s form [invariant functors][invariant], which means you can easily use an existing [`Codec`][codec] to encode and decode a different type, by mapping back-and-forth between the [`Codec`][codec]'s existing type argument and the new type. This becomes useful when dealing with newtypes, like `InstallationTime` in the following example.

```scala mdoc
final case class InstallationTime(value: Instant)

Codec[Instant].imap(InstallationTime(_))(_.value)
```

When we have a newtype where we ensure values are valid, we can use `imapError` instead.

```scala mdoc
import vulcan.AvroError

sealed abstract case class SerialNumber(value: String)

object SerialNumber {
  def apply(value: String): Either[AvroError, SerialNumber] =
    if(value.length == 12 && value.forall(_.isDigit))
      Right(new SerialNumber(value) {})
    else Left(AvroError(s"$value is not a serial number"))
}

Codec[String].imapError(SerialNumber(_))(_.value)
```

## Decimals

Avro decimals closely correspond to `BigDecimal`s with a fixed precision and scale.

`Codec.decimal` can be used to create a [`Codec`][codec] for `BigDecimal` given the precision and scale. When encoding, the [`Codec`][codec] checks the precision and scale of the `BigDecimal` to make sure it matches the precision and scale defined in the schema. When decoding, we check the precision is not exceeded.

```scala mdoc
Codec.decimal(precision = 10, scale = 2)
```

## Enumerations

Avro enumerations closely correspond to `sealed trait`s with `case object`s.

`Codec.deriveEnum` can be used to partly derive [`Codec`][codec]s for enumeration types.

```scala mdoc
import vulcan.{AvroDoc, AvroNamespace}

@AvroNamespace("com.example")
@AvroDoc("A selection of different fruits")
sealed trait Fruit
case object Apple extends Fruit
case object Banana extends Fruit
case object Cherry extends Fruit

Codec.deriveEnum[Fruit](
  symbols = List("apple", "banana", "cherry"),
  encode = {
    case Apple  => "apple"
    case Banana => "banana"
    case Cherry => "cherry"
  },
  decode = {
    case "apple"  => Right(Apple)
    case "banana" => Right(Banana)
    case "cherry" => Right(Cherry)
    case other    => Left(AvroError(s"$other is not a Fruit"))
  }
)
```

Annotations like `@AvroDoc` can be used to customize the derivation. There is no full derivation for enums, as it's highly recommended to use a library like [Enumeratum](modules.md#enumeratum) for enumerations, in which case we can easily use `Codec.deriveEnum` to derive [`Codec`][codec]s.

If we need more precise control of how enumerations are encoded, we can use `Codec.enum`.

```scala mdoc
Codec.enum[Fruit](
  name = "Fruit",
  namespace = Some("com.example"),
  doc = Some("A selection of different fruits"),
  symbols = List("apple", "banana", "cherry"),
  encode = {
    case Apple  => "apple"
    case Banana => "banana"
    case Cherry => "cherry"
  },
  decode = {
    case "apple"  => Right(Apple)
    case "banana" => Right(Banana)
    case "cherry" => Right(Cherry)
    case other    => Left(AvroError(s"$other is not a Fruit"))
  },
  default = Some(Banana)
)
```

## Records

Avro records closely correspond to `case class`es.

`Codec.derive` can be used to automatically derive [`Codec`][codec]s for `case class`es.

```scala mdoc
@AvroNamespace("com.example")
@AvroDoc("Person with a first name, last name, and optional age")
final case class Person(firstName: String, lastName: String, age: Option[Int])

Codec.derive[Person]
```

Annotations like `@AvroDoc` can be used to customize the derivation. If we need more precise control over how records are encoded, e.g. in order to specify default field values, we can instead use `Codec.record` and explicitly specify the encoding.

```scala mdoc
import cats.implicits._

Codec.record[Person](
  name = "Person",
  namespace = Some("com.example"),
  doc = Some("Person with a full name and optional age")
) { field =>
  field("fullName", p => s"${p.firstName} ${p.lastName}") *>
  (
    field("firstName", _.firstName),
    field("lastName", _.lastName, doc = Some("the last name")),
    field("age", _.age, default = Some(None))
  ).mapN(Person(_, _, _))
}
```

## Unions

Avro unions closely correspond to `sealed trait`s.

`Codec.derive` can be used to automatically derive [`Codec`][codec]s for `sealed trait`s.

```scala mdoc
sealed trait FirstOrSecond

@AvroNamespace("com.example")
final case class First(value: Int) extends FirstOrSecond

@AvroNamespace("com.example")
final case class Second(value: String) extends FirstOrSecond

Codec.derive[FirstOrSecond]
```

Shapeless `Coproduct`s are also encoded as unions.

```scala mdoc
import shapeless.{:+:, CNil}

Codec[Int :+: String :+: CNil]
```

`Option`s are also encoded as unions.

```scala mdoc
Codec[Option[Int]]
```

[avroerror]: @API_BASE_URL@/AvroError.html
[codec]: @API_BASE_URL@/Codec.html
[invariant]: https://typelevel.org/cats/typeclasses/invariant.html
