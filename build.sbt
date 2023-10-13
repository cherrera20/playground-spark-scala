name := Build.Name
ThisBuild / version := Build.Version
ThisBuild / description := Build.Description

lazy val root = (project in file("."))
  .enablePlugins(GitVersioning)
  .settings(Common.settings: _*)
  .settings(libraryDependencies ++= Dependencies.libraryDependencies)

/** ********* COMMANDS ALIASES ******************/
addCommandAlias("c", "clean")
addCommandAlias("t", "test")
addCommandAlias("to", "testOnly")
addCommandAlias("tq", "testQuick")
addCommandAlias("tsf", "testShowFailed")
addCommandAlias("co", "compile")
addCommandAlias("tc", "test:compile")
addCommandAlias("f", "scalafmt")             // Format production files according to ScalaFmt
addCommandAlias("fc", "scalafmtCheck")       // Check if production files are formatted according to ScalaFmt
addCommandAlias("tf", "test:scalafmt")       // Format test files according to ScalaFmt
addCommandAlias("tfc", "test:scalafmtCheck") // Check if test files are formatted according to ScalaFmt
addCommandAlias("cv", "coverage")            //
addCommandAlias("cvr", "coverageReport")     //

// All the needed tasks before pushing to the repository (compile, compile test, format check in prod and test)
//clean compile coverage test coverageReport
addCommandAlias("prep", ";c;co;cv;t;cvr")

import sbtassembly.MergeStrategy
assembly / assemblyMergeStrategy := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x                                                                     => (assembly / assemblyMergeStrategy).value(x)

}
