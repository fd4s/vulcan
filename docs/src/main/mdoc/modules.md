---
id: modules
title: Modules
---

The following sections describe the additional modules.

## Enumeratum

The `@ENUMERATUM_MODULE_NAME@` module provides [`Codec`][codec]s for enumerations.

For regular `Enum`s, also mix in `VulcanEnum` to derive a [`Codec`][codec] instance.

```scala mdoc
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

## Refined

The `@REFINED_MODULE_NAME@` module provides [`Codec`][codec]s for refinement types. Refinement types are encoded using their base type (e.g. `Int` for `PosInt`). When decoding, [`Codec`][codec]s check to ensure values conform to the predicate of the refinement type (e.g. `Positive` for `PosInt`), and raise an error for values which do not conform.

```scala mdoc
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import vulcan.{decode, encode}
import vulcan.refined._

Codec[PosInt]

encode[PosInt](1)

decode[PosInt](0)
```

[codec]: @API_BASE_URL@/Codec.html
