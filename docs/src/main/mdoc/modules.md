---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Enumeratum

The `@ENUMERATUM_MODULE_NAME@` module provides [`Codec`][codec]s for [Enumeratum](https://github.com/lloydmeta/enumeratum) enumerations.

For regular `Enum`s, also mix in `VulcanEnum` to derive a [`Codec`][codec] instance.

```scala mdoc:reset-object
import enumeratum.{Enum, EnumEntry, VulcanEnum}
import enumeratum.EnumEntry.Lowercase
import vulcan.{AvroDoc, AvroNamespace, Codec}

@AvroNamespace("com.example")
@AvroDoc("The different card suits")
sealed trait Suit extends EnumEntry with Lowercase

object Suit extends Enum[Suit] with VulcanEnum[Suit] {
  case object Clubs extends Suit
  case object Diamonds extends Suit
  case object Hearts extends Suit
  case object Spades extends Suit

  val values = findValues
}

Codec[Suit]
```

Annotations like `@AvroDoc` can be used to customize the derivation.

For `ValueEnum`s, also mix in the matching `VulcanValueEnum` to derive a [`Codec`][codec] instance.

```scala mdoc
import enumeratum.values.{StringEnum, StringEnumEntry, StringVulcanEnum}

@AvroNamespace("com.example")
@AvroDoc("The available colors")
sealed abstract class Color(val value: String) extends StringEnumEntry

object Color extends StringEnum[Color] with StringVulcanEnum[Color] {
  case object Red extends Color("red")
  case object Green extends Color("green")
  case object Blue extends Color("blue")

  val values = findValues
}

Codec[Color]
```

For `StringVulcanEnum`, the enumeration is encoded as an Avro enumeration. For the other `VulcanValueEnum`s (`ByteVulcanEnum`, `CharVulcanEnum`, `IntVulcanEnum`, `LongVulcanEnum`, and `ShortVulcanEnum`), the encoding relies on the encoding of the enumeration's value type (`Byte`, `Char`, `Int`, `Long`, and `Short`).

```scala mdoc
import enumeratum.values.{IntEnum, IntEnumEntry, IntVulcanEnum}

sealed abstract class Day(val value: Int) extends IntEnumEntry

object Day extends IntEnum[Day] with IntVulcanEnum[Day] {
  case object Monday extends Day(1)
  case object Tuesday extends Day(2)
  case object Wednesday extends Day(3)
  case object Thursday extends Day(4)
  case object Friday extends Day(5)
  case object Saturday extends Day(6)
  case object Sunday extends Day(7)

  val values = findValues
}

Codec[Day]
```

## Generic

The `@GENERIC_MODULE_NAME@` module provides generic derivation of [`Codec`][codec]s using [Magnolia](https://github.com/propensive/magnolia) for records and unions and reflection for enumerations.

To derive [`Codec`][codec]s for `case class`es or `sealed trait`s, we can use `Codec.derive`. Annotations like `@AvroDoc` and `@AvroNamespace` can be used to customize the documentation and namespace during derivation.

```scala mdoc
import vulcan.generic._

@AvroNamespace("com.example")
@AvroDoc("Person with a first name, last name, and optional age")
final case class Person(firstName: String, lastName: String, age: Option[Int])

Codec.derive[Person]
```

While `case class`es correspond to Avro records, `sealed trait`s correspond to unions.

```scala mdoc
sealed trait FirstOrSecond

@AvroNamespace("com.example")
final case class First(value: Int) extends FirstOrSecond

@AvroNamespace("com.example")
final case class Second(value: String) extends FirstOrSecond

Codec.derive[FirstOrSecond]
```

[Shapeless](https://github.com/milessabin/shapeless) `Coproduct`s are also supported and correspond to Avro unions.

```scala mdoc
import shapeless.{:+:, CNil}

Codec[Int :+: String :+: CNil]
```

`Codec.deriveEnum` can be used to partly derive [`Codec`][codec]s for enumeration types. Annotations like `@AvroDoc` can be used to customize the derivation.


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

## Refined

The `@REFINED_MODULE_NAME@` module provides [`Codec`][codec]s for [refined](https://github.com/fthomas/refined) refinement types.

Refinement types are encoded using their base type (e.g. `Int` for `PosInt`). When decoding, [`Codec`][codec]s check to ensure values conform to the predicate of the refinement type (e.g. `Positive` for `PosInt`), and raise an error for values which do not conform.

```scala mdoc
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import vulcan.refined._

Codec[PosInt]

Codec.encode[PosInt](1)

Codec.decode[PosInt](0)
```

[codec]: @API_BASE_URL@/Codec.html
