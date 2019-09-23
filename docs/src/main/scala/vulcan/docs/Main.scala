package vulcan.docs

import java.nio.file.{FileSystems, Path}
import scala.collection.Seq
import vulcan.build.info._

object Main {
  def sourceDirectoryPath(rest: String*): Path =
    FileSystems.getDefault.getPath(sourceDirectory.getAbsolutePath, rest: _*)

  def minorVersion(version: String): String = {
    val Array(major, minor, _) = version.split('.')
    s"$major.$minor"
  }

  def minorVersionsString(versions: Seq[String]): String = {
    val minorVersions = versions.map(minorVersion)
    if (minorVersions.size <= 2) minorVersions.mkString(" and ")
    else minorVersions.init.mkString(", ") ++ " and " ++ minorVersions.last
  }

  def main(args: Array[String]): Unit = {
    val scalaMinorVersion = minorVersion(scalaVersion)

    val settings = mdoc
      .MainSettings()
      .withSiteVariables {
        Map(
          "ORGANIZATION" -> organization,
          "CORE_MODULE_NAME" -> coreModuleName,
          "CORE_CROSS_SCALA_VERSIONS" -> minorVersionsString(coreCrossScalaVersions),
          "ENUMERATUM_MODULE_NAME" -> enumeratumModuleName,
          "ENUMERATUM_CROSS_SCALA_VERSIONS" -> minorVersionsString(enumeratumCrossScalaVersions),
          "GENERIC_MODULE_NAME" -> genericModuleName,
          "GENERIC_CROSS_SCALA_VERSIONS" -> minorVersionsString(genericCrossScalaVersions),
          "REFINED_MODULE_NAME" -> refinedModuleName,
          "REFINED_CROSS_SCALA_VERSIONS" -> minorVersionsString(refinedCrossScalaVersions),
          "LATEST_VERSION" -> latestVersion,
          "LATEST_MINOR_VERSION" -> minorVersion(latestVersion),
          "DOCS_SCALA_MINOR_VERSION" -> scalaMinorVersion,
          "SCALA_PUBLISH_VERSIONS" -> minorVersionsString(crossScalaVersions),
          "API_BASE_URL" -> s"/vulcan/api/vulcan",
          "AVRO_VERSION" -> avroVersion,
          "CATS_VERSION" -> catsVersion,
          "ENUMERATUM_VERSION" -> enumeratumVersion,
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
