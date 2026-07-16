package enumeratum.values

import vulcan.generic.{AvroDoc, AvroNamespace}

sealed abstract class CustomByteEnum(val value: Byte) extends ByteEnumEntry

object CustomByteEnum extends ByteEnum[CustomByteEnum] with ByteVulcanEnum[CustomByteEnum] {
  case object First extends CustomByteEnum(1)
  case object Second extends CustomByteEnum(2)
  case object Third extends CustomByteEnum(3)

  val values = findValues

  override def withValueOpt(i: Byte): Option[CustomByteEnum] =
    if (i == 3) None
    else super.withValueOpt(i)
}

sealed abstract class CustomCharEnum(val value: Char) extends CharEnumEntry

object CustomCharEnum extends CharEnum[CustomCharEnum] with CharVulcanEnum[CustomCharEnum] {
  case object First extends CustomCharEnum('1')
  case object Second extends CustomCharEnum('2')
  case object Third extends CustomCharEnum('3')

  val values = findValues

  override def withValueOpt(c: Char): Option[CustomCharEnum] =
    if (c == '3') None
    else super.withValueOpt(c)
}

sealed abstract class CustomIntEnum(val value: Int) extends IntEnumEntry

object CustomIntEnum extends IntEnum[CustomIntEnum] with IntVulcanEnum[CustomIntEnum] {
  case object First extends CustomIntEnum(1)
  case object Second extends CustomIntEnum(2)
  case object Third extends CustomIntEnum(3)

  val values = findValues

  override def withValueOpt(i: Int): Option[CustomIntEnum] =
    if (i == 3) None
    else super.withValueOpt(i)
}

sealed abstract class CustomLongEnum(val value: Long) extends LongEnumEntry

object CustomLongEnum extends LongEnum[CustomLongEnum] with LongVulcanEnum[CustomLongEnum] {
  case object First extends CustomLongEnum(1L)
  case object Second extends CustomLongEnum(2L)
  case object Third extends CustomLongEnum(3L)

  val values = findValues

  override def withValueOpt(l: Long): Option[CustomLongEnum] =
    if (l == 3L) None
    else super.withValueOpt(l)
}

sealed abstract class CustomShortEnum(val value: Short) extends ShortEnumEntry

object CustomShortEnum extends ShortEnum[CustomShortEnum] with ShortVulcanEnum[CustomShortEnum] {
  case object First extends CustomShortEnum(1)
  case object Second extends CustomShortEnum(2)
  case object Third extends CustomShortEnum(3)

  val values = findValues

  override def withValueOpt(s: Short): Option[CustomShortEnum] =
    if (s == 3) None
    else super.withValueOpt(s)
}

@AvroNamespace("com.example")
@AvroDoc("Custom enumeration")
sealed abstract class CustomStringEnum(val value: String) extends StringEnumEntry

object CustomStringEnum
    extends StringEnum[CustomStringEnum]
    with StringVulcanEnum[CustomStringEnum] {
  case object First extends CustomStringEnum("first")
  case object Second extends CustomStringEnum("second")
  case object Third extends CustomStringEnum("third")

  val values = findValues

  override def withValueOpt(s: String): Option[CustomStringEnum] =
    if (s == "third") None
    else super.withValueOpt(s)
}
