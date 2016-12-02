name := "SciGraph-spark"

version := "1.0"

scalaVersion := "2.11.8"

spName := "monarchinitiative/SciGraph-spark"

sparkVersion := "1.6.2"

sparkComponents ++= Seq("graphx")

spDependencies += "neo4j-contrib/neo4j-spark-connector:1.0.0-RC1"

