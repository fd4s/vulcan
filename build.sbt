val avroVersion = "1.10.1"

val catsVersion = "2.3.1"

val enumeratumVersion = "1.6.1"

val magnoliaVersion = "0.17.0"

val refinedVersion = "0.9.20"

val shapelessVersion = "2.3.3"

val scala212 = "2.12.13"

val scala213 = "2.13.4"

val scala3 = "3.0.0-M3"

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
    dependencySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.apache.avro" % "avro" % avroVersion,
        "org.typelevel" %% "cats-free" % catsVersion
      ) ++ {
        if (isDotty.value) Nil
        else Seq("org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided)
      }
    ),
    publishSettings,
    mimaSettings,
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
    publishSettings,
    mimaSettings,
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
      libraryDependencies ++= Seq(
        "com.propensive" %% "magnolia" % magnoliaVersion,
        "com.chuusai" %% "shapeless" % shapelessVersion,
        "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
      )
    ),
    publishSettings,
    mimaSettings,
    scalaSettings,
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
    publishSettings,
    mimaSettings,
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
  libraryDependencies ++= (Seq(
    "org.typelevel" %% "discipline-scalatest" % "2.1.1",
    "org.typelevel" %% "cats-testkit" % catsVersion,
    "org.slf4j" % "slf4j-nop" % "1.7.30"
  ).map(_ % Test) ++ {
    if (isDotty.value) Nil
    else
      Seq(
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.3.2" % Test,
        compilerPlugin(("org.typelevel" %% "kind-projector" % "0.11.2").cross(CrossVersion.full))
      )
  }),
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
  organization := "com.github.fd4s"
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
        s"${startYear.value.get}-${java.time.Year.now}",
        "OVO Energy Limited",
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
// restore after releasing 2.0
//    if (publishArtifact.value && !isDotty.value) {
//      Set(organization.value %% moduleName.value % (previousStableVersion in ThisBuild).value.get)
//    } else Set()
    Set()
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
          "-Ywarn-unused"
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
        Seq("-Wconf:msg=Block&src=test/scala/vulcan/generic/.*:silent")
      } else Seq()

    val scala3ScalacOptions =
      if (isDotty.value) {
        Seq(
          "-Ykind-projector",
          "-source:3.0-migration",
          "-Xignore-scala2-macros"
        )
      } else Seq()

    commonScalacOptions ++
      scala2ScalacOptions ++
      scala212ScalacOptions ++
      scala213ScalacOptions ++
      scala3ScalacOptions
  },
  scalacOptions in (Compile, console) --= Seq("-Xlint", "-Ywarn-unused"),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  unmanagedSourceDirectories in Compile += {
    val sourceDir = (sourceDirectory in Compile).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => sourceDir / "scala-2.12"
      case _             => sourceDir / "scala-2.13+"
    }
  }
)

lazy val testSettings = Seq(
  logBuffered in Test := false,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument("-oDF")
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
        val scalaVersions = (crossScalaVersions in core).value.map(scalaVersionOf)
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
    "+test",
    "+mimaReportBinaryIssues",
    "+scalafmtCheck",
    "scalafmtSbtCheck",
    "+headerCheck",
    "+doc",
    "docs/run"
  )
)
