package org.acadgild.graphxpack1

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object pagerankobj {

  def main(args: Array[String]): Unit = {
    
    //Instantiation of Spark Conf Class
    
    val conf = new SparkConf().setAppName("PageRankAlgorithm").setMaster("local")
    
    //Instantiation of SparkContext Object
    
    val sc = new SparkContext(conf)

    // Load the edges as a graph

    val graph = GraphLoader.edgeListFile(sc, "/home/acadgild/follower_demo.txt")
    //graph: org.apache.spark.graphx.Graph[Int,Int]
    
    // Run PageRank

    val ranks = graph.pageRank(0.0001).vertices
    //ranks: org.apache.spark.graphx.VertexRDD[Double]

    // Join the ranks with the user names

    val data = sc.textFile("/home/acadgild/users.txt")
    //data: org.apache.spark.rdd.RDD[String] 
    
    val users = data.map { line =>
    
                         val fields = line.split(",")
                         (fields(0).toLong, fields(1))
                         }
    //users: org.apache.spark.rdd.RDD[(Long, String)]

    val ranksByUsername = users.join(ranks).map { case (id, (username, rank)) => (username, rank) }
    //ranksByUsername: org.apache.spark.rdd.RDD[(String, Double)]
    
    // Print the result

    println(ranksByUsername.collect().mkString("\n"))
    
    //Starting the SPARKCONTEXT object
    
    sc.stop()

  }
}