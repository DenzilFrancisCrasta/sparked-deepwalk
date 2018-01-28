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

    def getSingleRandomWalks(walkLength: Int): RDD[List[Long]] = {

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


    def getRandomWalks(
      walkLength: Int, 
      walksPerVertex: Int): RDD[List[Long]] = {

      val walks = for (i <- 1 to walksPerVertex) 
        yield getSingleRandomWalks(walkLength) 
      walks.reduceLeft(_ union _)
      
    }  
     
    /** renders the graph using graphviz library */
    def render(filename: String, directory: String)  = { 

        import scala.collection.immutable.Map

        val gv = new com.liangdp.graphviz4s.Graph()

        val adj = adjacencyList.values.collect 

        for ( v <- 1 to adj.size) {
            gv.node(v.toString,label=v.toString)
        }   

        for ( i <- 0 to adj.size -1) {
            for ( e <- adj(i) )  {
                gv.edge((i+1).toString(),e.toString())
            }
        }   
        //println(gv.source())
        gv.render(engine="dot", format="png", fileName=filename, directory = directory)

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
