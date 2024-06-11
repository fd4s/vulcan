val avroVersion = "1.11.3"
val catsVersion = "2.10.0"
val disciplineScalaTestVersion = "2.2.0"
val enumeratumVersion = "1.7.3"
val jacksonVersion = "2.16.1"
val magnolia2Version = "0.17.0"
val magnolia3Version = "1.3.4"
val munitVersion = "0.7.29"
val refinedVersion = "0.11.0"
val scalaCollectionCompatVersion = "2.11.0"
val shapeless3Version = "3.3.0"
val shapelessVersion = "2.3.10"
val slf4jNopVersion = "2.0.10"

val scala212 = "2.12.18"
val scala213 = "2.13.12"
val scala3 = "3.3.1"

lazy val vulcan = project
  .in(file("."))
  .settings(
    mimaSettings(),
    scalaSettings,
    noPublishSettings,
    console := (core / Compile / console).value,
    Test / console := (core / Test / console).value
  )
  .aggregate(core, enumeratum, generic, refined)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "vulcan",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
        "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
        "org.apache.avro" % "avro" % avroVersion,
        "org.typelevel" %% "cats-free" % catsVersion
      ) ++ {
        if (scalaVersion.value.startsWith("3")) Nil
        else Seq("org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided)
      }
    ),
    scalatestSettings,
    publishSettings,
    mimaSettings(),
    scalaSettings ++ Seq(
      crossScalaVersions += scala3
    ),
    testSettings
  )

lazy val enumeratum = project
  .in(file("modules/enumeratum"))
  .settings(
    moduleName := "vulcan-enumeratum",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies += "com.beachape" %% "enumeratum" % enumeratumVersion
    ),
    scalatestSettings,
    publishSettings,
    mimaSettings(),
    scalaSettings,
    testSettings
  )
  .dependsOn(core, generic)

lazy val generic = project
  .in(file("modules/generic"))
  .settings(
    moduleName := "vulcan-generic",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= {
        if (scalaVersion.value.startsWith("2"))
          Seq(
            "com.propensive" %% "magnolia" % magnolia2Version,
            "com.chuusai" %% "shapeless" % shapelessVersion,
            "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
          )
        else
          Seq(
            "com.softwaremill.magnolia1_3" %% "magnolia" % magnolia3Version,
            "org.typelevel" %% "shapeless3-deriving" % shapeless3Version
          )
      }
    ),
    scalatestSettings,
    publishSettings,
    mimaSettings(excludeScala3 = true), // re-include scala 3 after publishing
    scalaSettings ++ Seq(
      crossScalaVersions += scala3
    ),
    // magnolia requires compilation with the -Yretain-trees flag to support case class field default values on Scala 3
    Test / scalacOptions ++= (if (CrossVersion.partialVersion(scalaVersion.value).exists(_._1 == 3))
                                Seq("-Yretain-trees")
                              else Nil),
    testSettings
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val refined = project
  .in(file("modules/refined"))
  .settings(
    moduleName := "vulcan-refined",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "eu.timepit" %% "refined" % refinedVersion,
        "eu.timepit" %% "refined-scalacheck" % refinedVersion % Test
      )
    ),
    // uses munit because Scalatest and Refined for Scala 3.0.0-RC2 have
    // incompatible scala-xml dependencies
    munitSettings,
    publishSettings,
    mimaSettings(),
    scalaSettings ++ Seq(
      crossScalaVersions += scala3
    ),
    testSettings
  )
  .dependsOn(core)

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "vulcan-docs",
    name := moduleName.value,
    dependencySettings,
    noPublishSettings,
    scalaSettings,
    mdocSettings,
    buildInfoSettings
  )
  .dependsOn(core, enumeratum, generic, refined)
  .enablePlugins(BuildInfoPlugin, DocusaurusPlugin, MdocPlugin, ScalaUnidocPlugin)

lazy val dependencySettings = Seq(
  libraryDependencies ++= {
    if (scalaVersion.value.startsWith("3")) Nil
    else {
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion % Test,
        compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.2").cross(CrossVersion.full))
      )
    }
  },
  pomPostProcess := { (node: xml.Node) =>
    new xml.transform.RuleTransformer(new xml.transform.RewriteRule {
      def scopedDependency(e: xml.Elem): Boolean =
        e.label == "dependency" && e.child.exists(_.label == "scope")

      override def transform(node: xml.Node): xml.NodeSeq =
        node match {
          case e: xml.Elem if scopedDependency(e) => Nil
          case _                                  => Seq(node)
        }
    }).transform(node).head
  }
)

lazy val scalatestSettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "discipline-scalatest" % disciplineScalaTestVersion,
    "org.typelevel" %% "cats-testkit" % catsVersion,
    "org.slf4j" % "slf4j-nop" % slf4jNopVersion
  ).map(_ % Test)
)

lazy val munitSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalameta" %% "munit" % munitVersion,
    "org.scalameta" %% "munit-scalacheck" % munitVersion,
    "org.slf4j" % "slf4j-nop" % slf4jNopVersion
  ).map(_ % Test),
  testFrameworks += new TestFramework("munit.Framework")
)

lazy val mdocSettings = Seq(
  mdoc := (Compile / run).evaluated,
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scalaVersion.value),
  ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(core, enumeratum, generic, refined),
  ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
  cleanFiles += (ScalaUnidoc / unidoc / target).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(Compile / unidoc)
    .dependsOn(ThisBuild / updateSiteVariables)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(Compile / unidoc)
      .dependsOn(ThisBuild / updateSiteVariables)
      .value,
  // format: off
  ScalaUnidoc / unidoc / scalacOptions ++= Seq(
    "-doc-source-url", s"https://github.com/fd4s/vulcan/tree/v${(ThisBuild / latestVersion).value}€{FILE_PATH}.scala",
    "-sourcepath", (LocalRootProject / baseDirectory).value.getAbsolutePath,
    "-doc-title", "Vulcan",
    "-doc-version", s"v${(ThisBuild / latestVersion).value}",
    "-groups"
  )
  // format: on
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "vulcan.build",
  buildInfoObject := "info",
  buildInfoKeys := {
    val magnolia: String =
      if (scalaVersion.value.startsWith("3")) magnolia3Version else magnolia2Version
    Seq[BuildInfoKey](
      scalaVersion,
      scalacOptions,
      sourceDirectory,
      ThisBuild / latestVersion,
      BuildInfoKey.map(ThisBuild / version) {
        case (_, v) => "latestSnapshotVersion" -> v
      },
      BuildInfoKey.map(core / moduleName) {
        case (k, v) => "core" ++ k.capitalize -> v
      },
      BuildInfoKey.map(core / crossScalaVersions) {
        case (k, v) => "core" ++ k.capitalize -> v
      },
      BuildInfoKey.map(enumeratum / moduleName) {
        case (k, v) => "enumeratum" ++ k.capitalize -> v
      },
      BuildInfoKey.map(enumeratum / crossScalaVersions) {
        case (k, v) => "enumeratum" ++ k.capitalize -> v
      },
      BuildInfoKey.map(generic / moduleName) {
        case (k, v) => "generic" ++ k.capitalize -> v
      },
      BuildInfoKey.map(generic / crossScalaVersions) {
        case (k, v) => "generic" ++ k.capitalize -> v
      },
      BuildInfoKey.map(refined / moduleName) {
        case (k, v) => "refined" ++ k.capitalize -> v
      },
      BuildInfoKey.map(refined / crossScalaVersions) {
        case (k, v) => "refined" ++ k.capitalize -> v
      },
      LocalRootProject / organization,
      core / crossScalaVersions,
      BuildInfoKey("avroVersion" -> avroVersion),
      BuildInfoKey("catsVersion" -> catsVersion),
      BuildInfoKey("enumeratumVersion" -> enumeratumVersion),
      BuildInfoKey("magnoliaVersion" -> magnolia),
      BuildInfoKey("refinedVersion" -> refinedVersion),
      BuildInfoKey("shapelessVersion" -> shapelessVersion)
    )
  }
)

lazy val metadataSettings = Seq(
  organization := "com.github.fd4s"
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    Test / publishArtifact := false,
    pomIncludeRepository := (_ => false),
    homepage := Some(url("https://fd4s.github.io/vulcan")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear := Some(2019),
    headerLicense := Some(
      de.heikoseeberger.sbtheader.License.ALv2(
        s"${startYear.value.get}-${java.time.Year.now}",
        "OVO Energy Limited",
        HeaderLicenseStyle.SpdxSyntax
      )
    ),
    headerSources / excludeFilter := HiddenFileFilter,
    developers := List(
      Developer(
        id = "vlovgr",
        name = "Viktor Lövgren",
        email = "github@vlovgr.se",
        url = url("https://vlovgr.se")
      )
    )
  )

def mimaSettings(excludeScala3: Boolean = false) = Seq(
  mimaPreviousArtifacts := {
    if (publishArtifact.value && !(excludeScala3 && scalaVersion.value.startsWith("3"))) {
      Set(organization.value %% moduleName.value % (ThisBuild / previousStableVersion).value.get)
    } else Set()
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("vulcan.internal.*"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("*"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("vulcan.Codec.withDecodingTypeName"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("vulcan.AvroError.decode*"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("vulcan.AvroError.encode*"),
      ProblemFilters.exclude[MissingClassProblem]("vulcan.Codec$Field$"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("vulcan.AvroException.*"),

      // package-private
      ProblemFilters.exclude[DirectMissingMethodProblem]("vulcan.Codec.instanceForTypes")
    )
    // format: on
  }
)

lazy val noPublishSettings =
  publishSettings ++ Seq(
    publish / skip := true,
    publishArtifact := false
  )

lazy val scalaSettings = Seq(
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala212, scala213),
  scalacOptions ++= {
    val commonScalacOptions =
      Seq(
        "-deprecation",
        "-encoding",
        "UTF-8",
        "-feature",
        "-unchecked",
        "-Xfatal-warnings",
        "-language:implicitConversions"
      )

    val scala2ScalacOptions =
      if (scalaVersion.value.startsWith("2.")) {
        Seq(
          "-language:higherKinds",
          "-Xlint",
          "-Ywarn-dead-code",
          "-Ywarn-numeric-widen",
          "-Ywarn-value-discard",
          "-Ywarn-unused",
          "-Wconf:cat=unused-nowarn:s"
        )
      } else Seq()

    val scala212ScalacOptions =
      if (scalaVersion.value.startsWith("2.12")) {
        Seq(
          "-Yno-adapted-args",
          "-Ypartial-unification"
        )
      } else Seq()

    val scala213ScalacOptions =
      if (scalaVersion.value.startsWith("2.13")) {
        Seq("-Wconf:msg=Block&src=test/scala-2/vulcan/generic/.*:silent")
      } else Seq()

    val scala3ScalacOptions =
      if (scalaVersion.value.startsWith("3")) {
        Seq(
          "-Ykind-projector"
        )
      } else Seq()

    commonScalacOptions ++
      scala2ScalacOptions ++
      scala212ScalacOptions ++
      scala213ScalacOptions ++
      scala3ScalacOptions
  },
  Compile / console / scalacOptions --= Seq("-Xlint", "-Ywarn-unused"),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  Compile / unmanagedSourceDirectories ++= {
    val sourceDir = (Compile / sourceDirectory).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq(sourceDir / "scala-2.12", sourceDir / "scala-2")
      case Some((2, 13)) => Seq(sourceDir / "scala-2.13+", sourceDir / "scala-2")
      case _             => Seq(sourceDir / "scala-2.13+", sourceDir / "scala-3")
    }
  },
  Test / unmanagedSourceDirectories ++= {
    val sourceDir = (Test / sourceDirectory).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq(sourceDir / "scala-2")
      case _            => Nil
    }
  }
)

lazy val testSettings = Seq(
  Test / logBuffered := false,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF")
)

def scalaVersionOf(version: String): String = {
  if (version.contains("-")) version
  else {
    val (major, minor) =
      CrossVersion.partialVersion(version).get
    s"$major.$minor"
  }
}

val latestVersion = settingKey[String]("Latest stable released version")
ThisBuild / latestVersion := {
  val snapshot = (ThisBuild / isSnapshot).value
  val stable = (ThisBuild / isVersionStable).value

  if (!snapshot && stable) {
    (ThisBuild / version).value
  } else {
    (ThisBuild / previousStableVersion).value.get
  }
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
ThisBuild / updateSiteVariables := {
  val file =
    (LocalRootProject / baseDirectory).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (LocalRootProject / organization).value,
      "coreModuleName" -> (core / moduleName).value,
      "latestVersion" -> (ThisBuild / latestVersion).value,
      "scalaPublishVersions" -> {
        val scalaVersions = (core / crossScalaVersions).value.map(scalaVersionOf)
        if (scalaVersions.size <= 2) scalaVersions.mkString(" and ")
        else scalaVersions.init.mkString(", ") ++ " and " ++ scalaVersions.last
      }
    )

  val fileHeader =
    "// Generated by sbt. Do not edit directly."

  val fileContents =
    variables.toList
      .sortBy { case (key, _) => key }
      .map { case (key, value) => s"  $key: '$value'" }
      .mkString(s"$fileHeader\nmodule.exports = {\n", ",\n", "\n};\n")

  IO.write(file, fileContents)
}

def addCommandsAlias(name: String, values: List[String]) =
  addCommandAlias(name, values.mkString(";", ";", ""))

addCommandsAlias(
  "validate",
  List(
    "+clean",
    "+scalafmtCheck",
    "scalafmtSbtCheck",
    "+headerCheck",
    "+test",
    "+mimaReportBinaryIssues",
    "+doc",
    "docs/run"
  )
)
