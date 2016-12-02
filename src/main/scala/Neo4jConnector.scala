import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.neo4j.spark._
import org.neo4j.driver.internal.InternalNode
import scala.collection.JavaConverters._

object Neo4jConnector {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Neo4j connector")
    val sc = new SparkContext(conf)

    val result = Neo4jTupleRDD(sc, query, Seq.empty)
    println(result.count)

    val subjectIds = result.map(r => {
      r.filter(entry => {
        entry._1 == "subject"
      })(0)._2.asInstanceOf[InternalNode].id
    }).collect
    //subjectIds.collect().map(println)

    subjectIds.map(subjectId => {
      val resultTaxon = Neo4jTupleRDD(sc, easyQuery, Seq("nodeid" -> subjectId.asInstanceOf[java.lang.Long])).collect
      val taxon = resultTaxon.map(taxon => taxon(0)._2).mkString(",")
      println(subjectId + " - " + taxon)
    })

    //subjectIds.zip(taxon).map(println)

  }

  val query = """
     MATCH path=(subject:gene)<-[:`http://purl.obolibrary.org/obo/GENO_0000610`|`http://purl.obolibrary.org/obo/GENO_0000653`|`http://purl.obolibrary.org/obo/GENO_0000414`|`http://purl.obolibrary.org/obo/GENO_0000641`|`http://purl.obolibrary.org/obo/GENO_0000652`|`http://purl.obolibrary.org/obo/GENO_0000443`|`http://purl.obolibrary.org/obo/GENO_0000651`|`http://purl.obolibrary.org/obo/GENO_0000408`|`http://purl.obolibrary.org/obo/GENO_0000639`|`http://purl.obolibrary.org/obo/GENO_0000418`]-(variant)-[:`http://purl.obolibrary.org/obo/RO_0001021`|`http://purl.obolibrary.org/obo/RO_0002452`|`http://purl.obolibrary.org/obo/RO_0002200`|`http://purl.obolibrary.org/obo/RO_0001020`|`http://purl.obolibrary.org/obo/GENO_0000841`|`http://purl.obolibrary.org/obo/RO_0003306`|`http://purl.obolibrary.org/obo/GENO_0000840`|`http://purl.obolibrary.org/obo/GENO_0000743`|`http://purl.obolibrary.org/obo/RO_0003308`|`http://purl.obolibrary.org/obo/RO_0003303`|`http://purl.obolibrary.org/obo/RO_0003302`|`http://purl.obolibrary.org/obo/RO_0002610`|`http://purl.obolibrary.org/obo/GENO_0000740`|`http://purl.obolibrary.org/obo/RO_0003305`|`http://purl.obolibrary.org/obo/RO_0002326`|`http://purl.obolibrary.org/obo/RO_0003304`|`http://purl.obolibrary.org/obo/RO_0002607`]->(object:disease)
     RETURN DISTINCT path,
     subject, object,
     'gene' AS subject_category,
     'disease' AS object_category,
     'inferred' as qualifier
     UNION        
     MATCH path=(subject:gene)<-[:`http://purl.obolibrary.org/obo/GENO_0000610`|`http://purl.obolibrary.org/obo/GENO_0000653`|`http://purl.obolibrary.org/obo/GENO_0000414`|`http://purl.obolibrary.org/obo/GENO_0000641`|`http://purl.obolibrary.org/obo/GENO_0000652`|`http://purl.obolibrary.org/obo/GENO_0000443`|`http://purl.obolibrary.org/obo/GENO_0000651`|`http://purl.obolibrary.org/obo/GENO_0000408`|`http://purl.obolibrary.org/obo/GENO_0000639`|`http://purl.obolibrary.org/obo/GENO_0000418`]-(variant)<-[:`http://purl.obolibrary.org/obo/IAO_0000407`|`http://purl.obolibrary.org/obo/GENO_0000382`|`http://purl.obolibrary.org/obo/RO_0002473`|`http://purl.obolibrary.org/obo/RO_0002551`|`http://purl.obolibrary.org/obo/OBI_0000643`|`http://purl.obolibrary.org/obo/RO_0002351`|`http://purl.obolibrary.org/obo/RO_0002230`|`http://purl.obolibrary.org/obo/GENO_0000654`|`http://purl.obolibrary.org/obo/GENO_0000650`|`http://www.obofoundry.org/ro/ro.owl#has_integral_part`|`http://purl.obolibrary.org/obo/RO_0002224`|`http://purl.obolibrary.org/obo/GENO_0000231`|`http://purl.obolibrary.org/obo/RO_0002104`|`http://purl.obolibrary.org/obo/RO_0002524`|`http://purl.obolibrary.org/obo/IAO_0000039`|`http://purl.obolibrary.org/obo/BFO_0000051`|`http://purl.obolibrary.org/obo/RO_0002180`|`http://www.obofoundry.org/ro/ro.owl#has_proper_part`|`http://purl.obolibrary.org/obo/RO_0002520`|`http://purl.obolibrary.org/obo/IAO_0000581`|`http://purl.obolibrary.org/obo/RO_0002516`|`http://purl.obolibrary.org/obo/GENO_0000385`|`http://purl.obolibrary.org/obo/GENO_0000783`|`http://www.obofoundry.org/ro/ro.owl#has_improper_part`|`http://purl.obolibrary.org/obo/RO_0002518`|`http://purl.obolibrary.org/obo/IAO_0000583`*]-(genotype:genotype)-[:`http://purl.obolibrary.org/obo/RO_0001021`|`http://purl.obolibrary.org/obo/RO_0002452`|`http://purl.obolibrary.org/obo/RO_0002200`|`http://purl.obolibrary.org/obo/RO_0001020`|`http://purl.obolibrary.org/obo/GENO_0000841`|`http://purl.obolibrary.org/obo/RO_0003306`|`http://purl.obolibrary.org/obo/GENO_0000840`|`http://purl.obolibrary.org/obo/GENO_0000743`|`http://purl.obolibrary.org/obo/RO_0003308`|`http://purl.obolibrary.org/obo/RO_0003303`|`http://purl.obolibrary.org/obo/RO_0003302`|`http://purl.obolibrary.org/obo/RO_0002610`|`http://purl.obolibrary.org/obo/GENO_0000740`|`http://purl.obolibrary.org/obo/RO_0003305`|`http://purl.obolibrary.org/obo/RO_0002326`|`http://purl.obolibrary.org/obo/RO_0003304`|`http://purl.obolibrary.org/obo/RO_0002607`]->(object:disease)         RETURN DISTINCT path,         subject, object,         'gene' AS subject_category,         'disease' AS object_category,         'inferred' as qualifier         UNION         MATCH path=(subject:gene)<-[:`http://purl.obolibrary.org/obo/GENO_0000610`|`http://purl.obolibrary.org/obo/GENO_0000653`|`http://purl.obolibrary.org/obo/GENO_0000414`|`http://purl.obolibrary.org/obo/GENO_0000641`|`http://purl.obolibrary.org/obo/GENO_0000652`|`http://purl.obolibrary.org/obo/GENO_0000443`|`http://purl.obolibrary.org/obo/GENO_0000651`|`http://purl.obolibrary.org/obo/GENO_0000408`|`http://purl.obolibrary.org/obo/GENO_0000639`|`http://purl.obolibrary.org/obo/GENO_0000418`]-(variant)<-[:`http://purl.obolibrary.org/obo/IAO_0000407`|`http://purl.obolibrary.org/obo/GENO_0000382`|`http://purl.obolibrary.org/obo/RO_0002473`|`http://purl.obolibrary.org/obo/RO_0002551`|`http://purl.obolibrary.org/obo/OBI_0000643`|`http://purl.obolibrary.org/obo/RO_0002351`|`http://purl.obolibrary.org/obo/RO_0002230`|`http://purl.obolibrary.org/obo/GENO_0000654`|`http://purl.obolibrary.org/obo/GENO_0000650`|`http://www.obofoundry.org/ro/ro.owl#has_integral_part`|`http://purl.obolibrary.org/obo/RO_0002224`|`http://purl.obolibrary.org/obo/GENO_0000231`|`http://purl.obolibrary.org/obo/RO_0002104`|`http://purl.obolibrary.org/obo/RO_0002524`|`http://purl.obolibrary.org/obo/IAO_0000039`|`http://purl.obolibrary.org/obo/BFO_0000051`|`http://purl.obolibrary.org/obo/RO_0002180`|`http://www.obofoundry.org/ro/ro.owl#has_proper_part`|`http://purl.obolibrary.org/obo/RO_0002520`|`http://purl.obolibrary.org/obo/IAO_0000581`|`http://purl.obolibrary.org/obo/RO_0002516`|`http://purl.obolibrary.org/obo/GENO_0000385`|`http://purl.obolibrary.org/obo/GENO_0000783`|`http://www.obofoundry.org/ro/ro.owl#has_improper_part`|`http://purl.obolibrary.org/obo/RO_0002518`|`http://purl.obolibrary.org/obo/IAO_0000583`*]-(genotype:genotype)<-[:`http://purl.obolibrary.org/obo/RO_0001000`|`http://purl.obolibrary.org/obo/GENO_0000222`*1..2]-(person)-[:`http://purl.obolibrary.org/obo/GENO_0000743`|`http://purl.obolibrary.org/obo/GENO_0000740`|`http://purl.obolibrary.org/obo/RO_0002452`|`http://purl.obolibrary.org/obo/RO_0002200`]->(object:disease)
     RETURN DISTINCT path,
     subject, object,
     'gene' AS subject_category,
     'disease' AS object_category,
     'inferred' as qualifier
    """

  val easyQuery = """
    MATCH (n)-[*1..2]-(dummy)
    WHERE id(n) = {nodeid}
    RETURN DISTINCT dummy.iri
    LIMIT 1
    """

  val taxonQuery = """
    MATCH (n)-[:equivalentClass|sameAs*]-()-[:subClassOf|type*0..]-()<-[:`http://purl.obolibrary.org/obo/RO_0002525`|`http://purl.obolibrary.org/obo/RO_0002517`|`http://purl.obolibrary.org/obo/RO_0002519`]-()-[:`http://purl.obolibrary.org/obo/GENO_0000610`|`http://purl.obolibrary.org/obo/GENO_0000653`|`http://purl.obolibrary.org/obo/GENO_0000414`|`http://purl.obolibrary.org/obo/GENO_0000641`|`http://purl.obolibrary.org/obo/GENO_0000652`|`http://purl.obolibrary.org/obo/GENO_0000443`|`http://purl.obolibrary.org/obo/GENO_0000651`|`http://purl.obolibrary.org/obo/GENO_0000408`|`http://purl.obolibrary.org/obo/GENO_0000418`|`http://purl.obolibrary.org/obo/GENO_0000222`|`http://purl.obolibrary.org/obo/RO_0001000`]->()-[:`http://purl.obolibrary.org/obo/RO_0002162`]->(taxon)
    WHERE id(n) = {nodeid}
    RETURN DISTINCT taxon.iri;"""
}