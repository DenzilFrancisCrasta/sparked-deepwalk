package com.nrl;

import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.io.Source
import org.apache.spark.rdd.RDD

class HyperGraph(edges: RDD[(Int, Int)]) {

    val adjacencyList = edges.groupByKey()
                             .mapValues(_.toArray)
                             .partitionBy(new HashPartitioner(30))
                             .persist(StorageLevel.MEMORY_AND_DISK)

    def getSingleRandomWalks(walkLength: Int): RDD[Array[Int]] = {

      // Bootstrap the random walk from every vertex 
      var keyedRandomWalks = adjacencyList.keys.map(id => { 
          val walk = new Array[Int](walkLength)
          walk(0) = id
          (id, walk)
        })
      
      // Grow the walk choosing a random neighbour uniformly at random
      for (iter <- 1 until walkLength) {
        val grownRandomWalks = 
          adjacencyList.join(keyedRandomWalks)
                       .map {
                          case (node_id, (neighbours, walk)) => {
                            val r = new Random()
                            val randomNeighbour = neighbours(r.nextInt(neighbours.size))
                            walk(iter) = randomNeighbour
                            (randomNeighbour, walk )
                          } 
                        }

         keyedRandomWalks.unpersist()
         keyedRandomWalks = grownRandomWalks

      }

      keyedRandomWalks.values
    }


    def getRandomWalks(
      walkLength: Int, 
      walksPerVertex: Int): RDD[Array[Int]] = {

//      val walks = for (i <- 1 to walksPerVertex) 
//        yield getSingleRandomWalks(walkLength) 
//      walks.reduceLeft(_ union _)
      
      // Bootstrap the random walk from every vertex 
      var keyedRandomWalks = adjacencyList.keys.flatMap(id => { 
        for (iter <- 1 to walksPerVertex) 
          yield {
            val walk = new Array[Int](walkLength)
            walk(0) = id
            (id, walk)
          }
        })
      
      // Grow the walk choosing a random neighbour uniformly at random
      for (iter <- 1 until walkLength) {
        val grownRandomWalks = 
          adjacencyList.join(keyedRandomWalks)
                       .map {
                          case (node_id, (neighbours, walk)) => {
                            val r = new Random()
                            val randomNeighbour = neighbours(r.nextInt(neighbours.size))
                            walk(iter) = randomNeighbour
                            (randomNeighbour, walk )
                          } 
                        }

         keyedRandomWalks.unpersist()
         keyedRandomWalks = grownRandomWalks

      }

      keyedRandomWalks.values
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
                if (i < e)
                  gv.edge((i+1).toString(),e.toString())
            }
        }   
        //println(gv.source())
        gv.render(engine="fdp", format="png", fileName=filename, directory = directory)

    }   

}

object HyperGraph {

  def edgeListFile (
      spark: SparkSession,
      path : String )
      : HyperGraph = {
    
      val edges = spark.sparkContext
                       .textFile(path, 8)
                       .flatMap { line => {
                            val fields = line.split(",")
                            val a = fields(0).toInt
                            val b = fields(1).toInt
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

      val edges:Array[(Int, Int)] 
            = lines.zipWithIndex
                   .flatMap{ case (line: String, i: Int) => {
                               val fields = line.trim().split(separator)
                               fields.map(_.toInt)
                                     .filter(_ == 1)
                                     .zipWithIndex
                                     .map((e: (Int, Int)) => {
                                       ((i + 1).toInt, (e._2 + 1).toInt)
                                      }) 
                            } // end case expression
                   }// end flat map to generate pair (src, dest) of edges 
                   .toArray
      

      new HyperGraph(spark.sparkContext.parallelize(edges))
    
  }
}
