package com.nrl

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap
import java.io._

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object SparkedDeepWalkApp {

    def writeCSVFile[T](
          path: String,
          schema: Array[String],
          data: Array[Array[T]]) = {
      val writer = new PrintWriter(new File(path))
      writer.write(schema.mkString(",")+"\n")
      data.foreach {(x: Array[T]) => writer.write(x.mkString(",")+"\n") }
      writer.close()
    }

    def saveVectors(
      path: String, 
      header: Array[Int],
      data: Map[String, Array[Float]]) = {
        
      val writer = new PrintWriter(new File(path))
      writer.write(header.mkString(" ")+"\n")
      data.foreach { 
        case (node:String, vec: Array[Float]) => 
          writer.write(node+" "+ vec.mkString(" ") +"\n")
      }
      writer.close()
    }
      

            
    
    def vertexVisitCounts(walks: RDD[List[Long]]): Array[Array[Long]] = {
      walks.flatMap((walk: List[Long]) => walk)
           .countByValue
           .values
           .groupBy(identity)
           .mapValues(_.size)
           .map(kv => Array(kv._1, kv._2))
           .toArray
      
    }

    def parseArguments(args: Array[String]): Map[String, String] = {
      if (args(0) != "Karate_Club") {
        Map(
          "DATASET_NAME"       -> args(0),
          "DATASET_DIR"        -> args(1),
          "NODES_FILE"         -> args(2),
          "EDGES_FILE"         -> args(3),
          "LABELS_FILE"        -> args(4),
          "NODE_TAG_FILE"      -> args(5),
          "OUTPUT_DIR"         -> args(6),
          "RANDOM_WALK_LENGTH" -> args(7),
          "NO_OF_RANDOM_WALKS" -> args(8),
          "VECTOR_DIM"         -> args(9),
          "NUM_PARTITIONS"     -> args(10),
          "NUM_ITERATIONS"     -> args(11),
          "WINDOW_SIZE"        -> args(12)
        )
      }
      else {
        Map(
          "DATASET_NAME"       -> args(0),
          "DATASET_DIR"        -> args(1),
          "DATASET_FILE"       -> args(2),
          "OUTPUT_DIR"         -> args(3),
          "RANDOM_WALK_LENGTH" -> args(4),
          "NO_OF_RANDOM_WALKS" -> args(5),
          "VECTOR_DIM"         -> args(6)
        )
        
      }
      
    }

    def main(args: Array[String]) {

      // setup spark session 
      val spark = SparkSession
                    .builder()
                    .appName("Sparked DeepWalk")
                    .getOrCreate()

      import spark.implicits._
      Logger.getRootLogger().setLevel(Level.ERROR)


      val config = parseArguments(args)

      // build the hypergraph from the serialized graph representations
      val G = if (config("DATASET_NAME") == "Karate_Club") {
                val filepath = config("DATASET_DIR") + config("DATASET_FILE");
                HyperGraph.adjacencyMatrixFile(spark, filepath)
              } else {
                val filepath = config("DATASET_DIR") + config("EDGES_FILE");
                HyperGraph.edgeListFile(spark, filepath)
              }


//      val nodes  = spark.read.textFile(config("DATASET_DIR") + config("NODES_FILE")).rdd.map(_.toLong)
 //     val labels = spark.read.textFile(config("DATASET_DIR") + config("LABELS_FILE")).rdd.map(_.toLong)

      //G.render(config("DATASET_NAME"), config("OUTPUT_DIR"))

      // generate random walks of configured length
      val randomWalks = G.getRandomWalks(
        config("RANDOM_WALK_LENGTH").toInt, 
        config("NO_OF_RANDOM_WALKS").toInt )
      randomWalks.persist()



       // println(config("DATASET_NAME"))
       // println("|V| " + nodes.count)
       // println("|E| " + edges.count)
       // println("|Y| " + labels.count)
       // println("Adjacency List |V|" + adjacencyList.count)
      println("Random Walk |V|" + randomWalks.count)


      val word2vec = (new Word2Vec())
        .setNumPartitions(config("NUM_PARTITIONS").toInt)
        .setNumIterations(config("NUM_ITERATIONS").toInt)
        .setVectorSize(config("VECTOR_DIM").toInt)
        .setWindowSize(config("WINDOW_SIZE").toInt)
      val model    = word2vec.fit(randomWalks.map(_.map(_.toString)))
      
      val vectors  = model.getVectors

      val vectorFile = config("OUTPUT_DIR") + config("DATASET_NAME")  + "_vec.txt"
      saveVectors(vectorFile, Array(vectors.size, config("VECTOR_DIM").toInt), vectors)


  //    val visits = vertexVisitCounts(randomWalks)
   //   val outputFile = config("OUTPUT_DIR") + config("DATASET_NAME") + "_vertex_visit_freq.csv"
  //    val schema = Array("numberOfVisits" ,"numberOfVertices") 
   //   writeCSVFile[Long](outputFile, schema, visits.toArray)

      spark.stop()

    }
}
