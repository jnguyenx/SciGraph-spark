# sbt compile assembly && /home/jnguyenxuan/soft/spark-1.6.2/bin/spark-submit  --conf spark.neo4j.bolt.url=bolt://neo4j.monarchinitiative.org:443 --class "Neo4jConnector" --master local[4] target/scala-2.11/SciGraph-spark-assembly-1.0.jar

sbt compile assembly && /home/jnguyenxuan/soft/spark-2.0.2/bin/spark-submit  --conf spark.neo4j.bolt.url=bolt://neo4j.monarchinitiative.org:443 --class "Neo4jConnector2" --master local[10] target/scala-2.11/SciGraph-spark-assembly-1.0.jar

