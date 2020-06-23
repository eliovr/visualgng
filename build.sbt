name := "visualgng"

version := "1.2"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"

libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"

libraryDependencies += "org.apache.zeppelin" % "zeppelin-interpreter" % "0.9.0-preview1" % "provided"
libraryDependencies += "org.apache.zeppelin" % "zeppelin-display" % "0.9.0-preview1"
libraryDependencies += "org.apache.zeppelin" % "zeppelin-angular" % "0.9.0-preview1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30" % Test
