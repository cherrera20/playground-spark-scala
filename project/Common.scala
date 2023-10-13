import com.typesafe.sbt.SbtGit.git
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys._
import sbt.Package.ManifestAttributes

object Common {
  lazy val artifactoryHost =
    "artifactory.mpi-internal.com"

  lazy val settings: Seq[Def.Setting[_]] = Seq(
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    credentials ++= (for {
      username <- sys.env.get("ARTIFACTORY_USER")
      password <- sys.env.get("ARTIFACTORY_PWD")
    } yield Credentials(realm = "Artifactory Realm", host = artifactoryHost, username, password)),
    organization := "com.schibsted.spain.di",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalaVersion := "2.12.14",
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-explaintypes",
      "-feature",
      "-unchecked",
      "-Ywarn-dead-code",
      // "-Xfatal-warnings", Remove spark.read.json from SchemaConverterServiceTest
      "-Xlint:unsound-match"
    ),
    Test / console / scalacOptions := ((Compile / console) / scalacOptions).value,
    javaOptions ++= Seq(
      "-XX:+UnlockExperimentalVMOptions",
      "-XX:+UseCGroupMemoryLimitForHeap"
    ),
    // Testing
    Test / testForkedParallel := false,
    Test / fork := true,
    Test / parallelExecution := false,
    // Assembly
    assembly / test := {},
    assemblyJarName := Build.Name + "-" + Build.Version + ".jar",
    packageOptions := Seq(ManifestAttributes(("Repository-Commit", git.gitHeadCommit.value.get))),
    // ScalaStyle
    scalastyleFailOnError := true,
    scalastyleFailOnWarning := true,
    scalastyleConfig := file("scalastyle_config.xml"),
    compileScalastyle := (Compile / scalastyle).toTask("").value,
    (Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value,
    (Test / scalastyleFailOnError) := true,
    (Test / scalastyleFailOnWarning) := true,
    (Test / scalastyleConfig) := file("scalastyle_config.xml"),
    testScalaStyle := (Test / scalastyle).toTask("").value,
    (Test / test) := ((Test / test) dependsOn testScalaStyle).value
  )

  lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
  lazy val testScalaStyle    = taskKey[Unit]("testScalastyle")
}
