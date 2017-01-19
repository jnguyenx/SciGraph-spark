import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.neo4j.spark._
import org.neo4j.driver.internal.InternalNode
import scala.collection.JavaConverters._

object Neo4jConnectorGraph {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Neo4j connector")
    val sc = new SparkContext(conf)

    val neo = Neo4j(sc)

    //val graphQuery = "MATCH path=(d:disease)-[r]-(p:Phenotype) RETURN id(d) as disease, id(p) as phenotype, type(r) LIMIT 100"
    //val graph = neo.rels(graphQuery).partitions(7).batch(200).loadGraphFrame

    //val graphFrame = neo.pattern(("disease","id"),("http://purl.obolibrary.org/obo/RO_0002200",null), ("Phenotype","id")).rows(1000000).loadGraphFrame

    val graphFrame = neo.cypher("MATCH (d:disease)-[r:`http://purl.obolibrary.org/obo/RO_0002200`]->(p:Phenotype) RETURN id(d) as source, id(p) as target, type(r)")

    println(graphFrame.loadRowRdd.count())

    //    println("vertices:" + graphFrame.vertices.count)
    //    println("edges:" + graphFrame.edges.count)

    //graphFrame.edges.foreach(println(_))

  }

}