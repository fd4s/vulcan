---
id: modules
title: Modules
---

The following sections describe the additional modules.

# Refined

The `@REFINED_MODULE_NAME@` module provides [`Codec`][codec]s for refinement types. Refinement types are encoded using their base type (e.g. `Int` for `PosInt`). When decoding, [`Codec`][codec]s check to ensure values conform to the predicate of the refinement type (e.g. `Positive` for `PosInt`), and raise an error for values which do not conform.

```scala mdoc
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import vulcan.{Codec, decode, encode}
import vulcan.refined._

Codec[PosInt]

encode[PosInt](1)

decode[PosInt](0)
```

[codec]: @API_BASE_URL@/Codec.html
