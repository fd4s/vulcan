package vulcan.docs

import java.nio.file.{FileSystems, Path}
import scala.collection.Seq
import vulcan.build.info._

object Main {
  def sourceDirectoryPath(rest: String*): Path =
    FileSystems.getDefault.getPath(sourceDirectory.getAbsolutePath, rest: _*)

  def majorVersion(version: String): String = {
    val parts = version.split('.')
    val major = parts(0)
    major
  }

  def scalaVersionOf(version: String): String = {
    if (version.contains("-")) version
    else {
      val parts = version.split('.')
      val (major, minor) = (parts(0), parts(1))
      s"$major.$minor"
    }
  }

  def scalaVersionsString(versions: Seq[String]): String = {
    val scalaVersions = versions.map(scalaVersionOf)
    if (scalaVersions.size <= 2) scalaVersions.mkString(" and ")
    else scalaVersions.init.mkString(", ") ++ " and " ++ scalaVersions.last
  }

  def main(args: Array[String]): Unit = {
    val scalaDocsVersion =
      scalaVersionOf(scalaVersion)

    val settings = mdoc
      .MainSettings()
      .withSiteVariables {
        Map(
          "ORGANIZATION" -> organization,
          "CORE_MODULE_NAME" -> coreModuleName,
          "CORE_CROSS_SCALA_VERSIONS" -> scalaVersionsString(coreCrossScalaVersions),
          "ENUMERATUM_MODULE_NAME" -> enumeratumModuleName,
          "ENUMERATUM_CROSS_SCALA_VERSIONS" -> scalaVersionsString(enumeratumCrossScalaVersions),
          "FUUID_MODULE_NAME" -> fuuidModuleName,
          "FUUID_CROSS_SCALA_VERSIONS" -> scalaVersionsString(fuuidCrossScalaVersions),
          "GENERIC_MODULE_NAME" -> genericModuleName,
          "GENERIC_CROSS_SCALA_VERSIONS" -> scalaVersionsString(genericCrossScalaVersions),
          "REFINED_MODULE_NAME" -> refinedModuleName,
          "REFINED_CROSS_SCALA_VERSIONS" -> scalaVersionsString(refinedCrossScalaVersions),
          "LATEST_VERSION" -> latestVersion,
          "LATEST_SNAPSHOT_VERSION" -> latestSnapshotVersion,
          "LATEST_MAJOR_VERSION" -> majorVersion(latestVersion),
          "DOCS_SCALA_VERSION" -> scalaDocsVersion,
          "SCALA_PUBLISH_VERSIONS" -> scalaVersionsString(crossScalaVersions),
          "API_BASE_URL" -> s"/vulcan/api/vulcan",
          "AVRO_VERSION" -> avroVersion,
          "CATS_VERSION" -> catsVersion,
          "ENUMERATUM_VERSION" -> enumeratumVersion,
          "FUUID_VERSION" -> fuuidVersion,
          "MAGNOLIA_VERSION" -> magnoliaVersion,
          "REFINED_VERSION" -> refinedVersion,
          "SHAPELESS_VERSION" -> shapelessVersion
        )
      }
      .withScalacOptions(scalacOptions.mkString(" "))
      .withIn(sourceDirectoryPath("main", "mdoc"))
      .withArgs(args.toList)

    val exitCode = mdoc.Main.process(settings)
    if (exitCode != 0) sys.exit(exitCode)
  }
}
