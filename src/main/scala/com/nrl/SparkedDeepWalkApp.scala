package com.nrl

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import scala.util.Random
import java.io._

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object SparkedDeepWalkApp {

    def parseArguments(args: Array[String]): Map[String, String] = {
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

    def main(args: Array[String]) {

      val spark = SparkSession
                    .builder()
                    .appName("Sparked DeepWalk")
                    .getOrCreate()

      import spark.implicits._

      Logger.getRootLogger().setLevel(Level.ERROR)


      val config = parseArguments(args)

      val edges = spark.read.textFile(config("DATASET_DIR") + config("EDGES_FILE")).rdd
                       .flatMap { line => {
                            val fields = line.split(",")
                            val a = fields(0).toLong
                            val b = fields(1).toLong
                            Array((a,b), (b,a))
                         }
                       }

      val nodes  = spark.read.textFile(config("DATASET_DIR") + config("NODES_FILE")).rdd.map(_.toLong)
      val labels = spark.read.textFile(config("DATASET_DIR") + config("LABELS_FILE")).rdd.map(_.toLong)

      val adjacencyList = edges.groupByKey()
                               .mapValues(_.toArray)
                               .partitionBy(new HashPartitioner(100))
                               .persist()



      var keyedRandomWalks = adjacencyList.keys.map(id => (id, List(id)))
      
      for (iter <- 1 until config("RANDOM_WALK_LENGTH").toInt) {
        keyedRandomWalks = adjacencyList.join(keyedRandomWalks)
                        .map {
                          case (node_id, (neighbours, walkSoFar)) => {
                            val r = new Random()
                            val randomNeighbour = neighbours(r.nextInt(neighbours.size))
                            (randomNeighbour, randomNeighbour :: walkSoFar )
                          } 
                        }

      }

      val randomWalks = keyedRandomWalks.values.persist()

      val vertexVisitCounts = randomWalks.flatMap((walk: List[Long]) => walk)
                                          .countByValue
                                          .values
                                          .groupBy(identity)
                                          .mapValues(_.size)

      val writer = new PrintWriter(new File(config("OUTPUT_DIR") + config("DATASET_NAME") + "_vertex_visit_freq.csv"))
      writer.write("numberOfVisits,numberOfVertices\n")
      vertexVisitCounts.foreach {
        case (k, v) => 
          writer.write(k +","+ v+"\n")
      }
      writer.close()

      println(config("DATASET_NAME"))
      println("|V| " + nodes.count)
      println("|E| " + edges.count)
      println("|Y| " + labels.count)
      println("Adjacency List |V|" + adjacencyList.count)
      println("Random Walk |V|" + randomWalks.count)


      val word2vec = new Word2Vec()
      val model    = word2vec.fit(randomWalks.map(_.map(_.toString)))
      val vectors  = model.getVectors

      vectors.take(2).foreach(println)



      spark.stop()

    }
}
