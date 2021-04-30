package vulcan

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BaseSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues
