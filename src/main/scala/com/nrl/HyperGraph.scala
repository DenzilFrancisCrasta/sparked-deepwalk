package com.nrl;

import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
import scala.util.Random
import scala.io.Source
import org.apache.spark.rdd.RDD

class HyperGraph(edges: RDD[(Long, Long)]) {

    val adjacencyList = edges.groupByKey()
                             .mapValues(_.toArray)
                             .partitionBy(new HashPartitioner(100))
                             .persist()

    def getRandomWalks(walkLength: Int): RDD[List[Long]] = {

      // Bootstrap the random walk from every vertex 
      var keyedRandomWalks = adjacencyList.keys.map(id => (id, List(id)))
      
      // Grow the walk choosing a random neighbour uniformly at random
      for (iter <- 1 until walkLength) {
        keyedRandomWalks = 
          adjacencyList.join(keyedRandomWalks)
                       .map {
                          case (node_id, (neighbours, walkSoFar)) => {
                            val r = new Random()
                            val randomNeighbour = neighbours(r.nextInt(neighbours.size))
                            (randomNeighbour, randomNeighbour :: walkSoFar )
                          } 
                        }

      }

      keyedRandomWalks.values
    }
     
    /** renders the graph using graphviz library */
    def render(filename: String, directory: String)  = { 
      /*
        val gv = new com.liangdp.graphviz4s.Graph()
        var k = 0 
        for ( v <- vertexLabels) {
            gv.node(k.toString(),label=v,attrs=Map("shape"->"plaintext"))
                k = k+1 
        }   

        for ( i <- 0 to adjacencyList.length-1) {
            for ( e <- adjacencyList(i) )  {
                if( e._1 > i) {
                    val col = new StringBuilder("\"black")
                        for (_ <- 2 to e._2 ) { 
                            col.append(":white:black")
                        }
                    col.append("\"")
                        gv.edge(i.toString(),e._1.toString(),attrs=Map("color"->col.toString()))
                }
            }
        }   
        println(gv.source())
        gv.render(engine="neato", format="png", fileName=filename, directory = directory)

        */
    }   

}

object HyperGraph {

  def edgeListFile (
      spark: SparkSession,
      path : String )
      : HyperGraph = {
    
      val edges = spark.read.textFile(path).rdd
                       .flatMap { line => {
                            val fields = line.split(",")
                            val a = fields(0).toLong
                            val b = fields(1).toLong
                            Array((a,b), (b,a))
                         }
                       }

      new HyperGraph(edges)
  }

  def adjacencyMatrixFile (
      spark: SparkSession,
      path : String,
      separator: String =" ")
      : HyperGraph = {

      val lines = Source.fromFile(path).getLines()

      val edges:Array[(Long, Long)] 
            = lines.zipWithIndex
                   .flatMap{ case (line: String, i: Int) => {
                               val fields = line.trim().split(separator)
                               fields.map(_.toLong)
                                     .filter(_ == 1)
                                     .zipWithIndex
                                     .map((e: (Long, Int)) => {
                                       ((i + 1).toLong, (e._2 + 1).toLong)
                                      }) 
                            } // end case expression
                   }// end flat map to generate pair (src, dest) of edges 
                   .toArray
      

      new HyperGraph(spark.sparkContext.parallelize(edges))
    
  }
      

  
  
}
