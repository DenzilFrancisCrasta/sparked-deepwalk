package com.nrl

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import java.io._

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object SparkedDeepWalkApp {

    def writeCSVFile(
          path: String,
          schema: Array[String],
          data: Array[(Long, Long)]) = {
      val writer = new PrintWriter(new File(path))
      writer.write(schema.mkString(",")+"\n")
      data.foreach {
        case (k, v) => 
          writer.write(k +","+ v+"\n")
      }
      writer.close()
    }

            
    
    def vertexVisitCounts(walks: RDD[List[Long]]): Map[Long, Long] = {
      walks.flatMap((walk: List[Long]) => walk)
           .countByValue
           .values
           .groupBy(identity)
           .mapValues(_.size)
      
    }

    def parseArguments(args: Array[String]): Map[String, String] = {
      if (args(0) != "karate") {
        Map(
          "DATASET_NAME"  -> args(0),
          "DATASET_DIR"   -> args(1),
          "NODES_FILE"    -> args(2),
          "EDGES_FILE"    -> args(3),
          "LABELS_FILE"   -> args(4),
          "NODE_TAG_FILE" -> args(5),
          "OUTPUT_DIR"    -> args(6),
          "RANDOM_WALK_LENGTH" -> args(7),
          "NO_OF_RANDOM_WALKS" -> args(8)
        )
      }
      else {
        Map(
          "DATASET_NAME"  -> args(0),
          "DATASET_DIR"   -> args(1),
          "DATASET_FILE"  -> args(2),
          "OUTPUT_DIR"    -> args(6),
          "RANDOM_WALK_LENGTH" -> args(7),
          "NO_OF_RANDOM_WALKS" -> args(8)
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
      val G = if (config("DATASET_NAME") == "karate") {
                val filepath = config("DATASET_DIR") + config("EDGES_FILE");
                HyperGraph.adjacencyMatrixFile(spark, filepath)
              } else {
                val filepath = config("DATASET_DIR") + config("EDGES_FILE");
                HyperGraph.edgeListFile(spark, filepath)
              }


//      val nodes  = spark.read.textFile(config("DATASET_DIR") + config("NODES_FILE")).rdd.map(_.toLong)
 //     val labels = spark.read.textFile(config("DATASET_DIR") + config("LABELS_FILE")).rdd.map(_.toLong)


      // generate random walks of configured length
      val randomWalks = G.getRandomWalks(config("RANDOM_WALK_LENGTH").toInt)
      randomWalks.persist()



     // println(config("DATASET_NAME"))
//      println("|V| " + nodes.count)
 //     println("|E| " + edges.count)
  //    println("|Y| " + labels.count)
    //  println("Adjacency List |V|" + adjacencyList.count)
      println("Random Walk |V|" + randomWalks.count)


      val word2vec = new Word2Vec()
      val model    = word2vec.fit(randomWalks.map(_.map(_.toString)))
      val vectors  = model.getVectors

      vectors.take(2).foreach(println)

      val visits = vertexVisitCounts(randomWalks)
      val outputFile = config("OUTPUT_DIR") + config("DATASET_NAME") + "_vertex_visit_freq.csv"
      val schema = Array("numberOfVisits" ,"numberOfVertices") 
      writeCSVFile(outputFile, schema, visits.toArray)

      spark.stop()

    }
}
