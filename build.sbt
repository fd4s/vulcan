val avroVersion = "1.9.1"

val catsVersion = "2.0.0"

val enumeratumVersion = "1.5.14"

val magnoliaVersion = "0.12.5"

val refinedVersion = "0.9.10"

val shapelessVersion = "2.3.3"

val scala212 = "2.12.10"

val scala213 = "2.13.1"

lazy val vulcan = project
  .in(file("."))
  .settings(
    mimaSettings,
    scalaSettings,
    noPublishSettings,
    console := (console in (core, Compile)).value,
    console in Test := (console in (core, Test)).value
  )
  .aggregate(core, enumeratum, generic, refined)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    moduleName := "vulcan",
    name := moduleName.value,
    dependencySettings,
    publishSettings,
    mimaSettings,
    scalaSettings,
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
    publishSettings,
    mimaSettings,
    scalaSettings,
    testSettings
  )
  .dependsOn(core)

lazy val generic = project
  .in(file("modules/generic"))
  .settings(
    moduleName := "vulcan-generic",
    name := moduleName.value,
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "com.propensive" %% "magnolia" % magnoliaVersion,
        "com.chuusai" %% "shapeless" % shapelessVersion
      )
    ),
    publishSettings,
    mimaSettings,
    scalaSettings,
    testSettings ++ Seq(
      // Magnolia uses deprecated Either#left.get on Scala 2.13
      scalacOptions in Test -= "-Xfatal-warnings"
    )
  )
  .dependsOn(core)

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
    publishSettings,
    mimaSettings,
    scalaSettings,
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
  libraryDependencies ++= Seq(
    "org.apache.avro" % "avro" % avroVersion,
    "org.typelevel" %% "cats-free" % catsVersion,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value
  ),
  libraryDependencies ++= Seq(
    "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2",
    "org.typelevel" %% "discipline-scalatest" % "1.0.0-RC1",
    "org.typelevel" %% "cats-testkit" % catsVersion,
    "org.slf4j" % "slf4j-nop" % "1.7.30"
  ).map(_ % Test),
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full)
)

lazy val mdocSettings = Seq(
  mdoc := run.in(Compile).evaluated,
  scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused"),
  crossScalaVersions := Seq(scalaVersion.value),
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(core, enumeratum, generic, refined),
  target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
  cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
  docusaurusCreateSite := docusaurusCreateSite
    .dependsOn(unidoc in Compile)
    .dependsOn(updateSiteVariables in ThisBuild)
    .value,
  docusaurusPublishGhpages :=
    docusaurusPublishGhpages
      .dependsOn(unidoc in Compile)
      .dependsOn(updateSiteVariables in ThisBuild)
      .value,
  // format: off
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", s"https://github.com/fd4s/vulcan/tree/v${(latestVersion in ThisBuild).value}€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-doc-title", "Vulcan",
    "-doc-version", s"v${(latestVersion in ThisBuild).value}",
    "-groups"
  )
  // format: on
)

lazy val buildInfoSettings = Seq(
  buildInfoPackage := "vulcan.build",
  buildInfoObject := "info",
  buildInfoKeys := Seq[BuildInfoKey](
    scalaVersion,
    scalacOptions,
    sourceDirectory,
    latestVersion in ThisBuild,
    BuildInfoKey.map(version in ThisBuild) {
      case (_, v) => "latestSnapshotVersion" -> v
    },
    BuildInfoKey.map(moduleName in core) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in core) {
      case (k, v) => "core" ++ k.capitalize -> v
    },
    BuildInfoKey.map(moduleName in enumeratum) {
      case (k, v) => "enumeratum" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in enumeratum) {
      case (k, v) => "enumeratum" ++ k.capitalize -> v
    },
    BuildInfoKey.map(moduleName in generic) {
      case (k, v) => "generic" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in generic) {
      case (k, v) => "generic" ++ k.capitalize -> v
    },
    BuildInfoKey.map(moduleName in refined) {
      case (k, v) => "refined" ++ k.capitalize -> v
    },
    BuildInfoKey.map(crossScalaVersions in refined) {
      case (k, v) => "refined" ++ k.capitalize -> v
    },
    organization in LocalRootProject,
    crossScalaVersions in core,
    BuildInfoKey("avroVersion" -> avroVersion),
    BuildInfoKey("catsVersion" -> catsVersion),
    BuildInfoKey("enumeratumVersion" -> enumeratumVersion),
    BuildInfoKey("magnoliaVersion" -> magnoliaVersion),
    BuildInfoKey("refinedVersion" -> refinedVersion),
    BuildInfoKey("shapelessVersion" -> shapelessVersion)
  )
)

lazy val metadataSettings = Seq(
  organization := "com.ovoenergy",
  organizationName := "OVO Energy Limited",
  organizationHomepage := Some(url("https://ovoenergy.com"))
)

lazy val publishSettings =
  metadataSettings ++ Seq(
    publishArtifact in Test := false,
    pomIncludeRepository := (_ => false),
    homepage := Some(url("https://fd4s.github.io/vulcan")),
    licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    startYear := Some(2019),
    headerLicense := Some(
      de.heikoseeberger.sbtheader.License.ALv2(
        s"${startYear.value.get}",
        organizationName.value,
        HeaderLicenseStyle.SpdxSyntax
      )
    ),
    excludeFilter.in(headerSources) := HiddenFileFilter,
    developers := List(
      Developer(
        id = "vlovgr",
        name = "Viktor Lövgren",
        email = "github@vlovgr.se",
        url = url("https://vlovgr.se")
      )
    )
  )

lazy val mimaSettings = Seq(
  mimaPreviousArtifacts := {
    if (publishArtifact.value) {
      // Set(organization.value %% moduleName.value % (previousStableVersion in ThisBuild).value.get)
      Set() // Re-enable once 1.0.0 has been released
    } else Set()
  },
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._
    // format: off
    Seq(
      ProblemFilters.exclude[Problem]("vulcan.internal.*"),
      ProblemFilters.exclude[IncompatibleSignatureProblem]("*")
    )
    // format: on
  }
)

lazy val noPublishSettings =
  publishSettings ++ Seq(
    skip in publish := true,
    publishArtifact := false
  )

lazy val scalaSettings = Seq(
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala212, scala213),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Ywarn-unused",
    "-Ypartial-unification"
  ).filter {
    case ("-Yno-adapted-args" | "-Ypartial-unification") if scalaVersion.value.startsWith("2.13") =>
      false
    case _ => true
  },
  scalacOptions in (Compile, console) --= Seq("-Xlint", "-Ywarn-unused"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)

lazy val testSettings = Seq(
  logBuffered in Test := false,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDF")
)

def minorVersion(version: String): String = {
  val (major, minor) =
    CrossVersion.partialVersion(version).get
  s"$major.$minor"
}

val latestVersion = settingKey[String]("Latest stable released version")
latestVersion in ThisBuild := {
  val snapshot = (isSnapshot in ThisBuild).value
  val stable = (isVersionStable in ThisBuild).value

  if (!snapshot && stable) {
    (version in ThisBuild).value
  } else {
    (previousStableVersion in ThisBuild).value.get
  }
}

val updateSiteVariables = taskKey[Unit]("Update site variables")
updateSiteVariables in ThisBuild := {
  val file =
    (baseDirectory in LocalRootProject).value / "website" / "variables.js"

  val variables =
    Map[String, String](
      "organization" -> (organization in LocalRootProject).value,
      "coreModuleName" -> (moduleName in core).value,
      "latestVersion" -> (latestVersion in ThisBuild).value,
      "scalaPublishVersions" -> {
        val minorVersions = (crossScalaVersions in core).value.map(minorVersion)
        if (minorVersions.size <= 2) minorVersions.mkString(" and ")
        else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
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
    "+coverage",
    "+test",
    "+coverageReport",
    "+mimaReportBinaryIssues",
    "+scalafmtCheck",
    "scalafmtSbtCheck",
    "+headerCheck",
    "+doc",
    "docs/run"
  )
)
