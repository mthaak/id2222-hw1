name := "SimilarItems"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"
val sparkXMLVersion = "0.3.3"
val sparkCsvVersion = "1.4.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// https://hadoopist.wordpress.com/2016/08/09/creat-a-simple-build-sbt-file-for-a-spark-project/
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("jline", "2.12"),
)