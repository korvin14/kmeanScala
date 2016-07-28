package bigdata.ml

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object App {

  def distanceSquare(p1: (Double, Double), p2: (Double, Double)): Double = {
    //TODO return || p1 - p2 || ^ 2
    return (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
  }

  def findNearestCenter(centers: Array[(Double, Double)], p: (Double, Double)): (Double, Int) = {
    val distanceWithIndex = centers.zipWithIndex.map {
      case (c, i) => (distanceSquare(c, p), i)
    }
    // TODO : Find nearest center from point p. Return the distance and the center index
    distanceWithIndex.reduce {
      (f1, f2) =>
        {
          if (f1._1 > f2._1) {
            f2
          } else {
            f1
          }
        }

    }
//    val best_distance = Double.MaxValue
//    val best_center_id = -1
  }

  def main(args: Array[String]) {
    println("Arguments = " + args.mkString(" "))

    val infile = args(0) // path of order file
    val K = args(1).toInt // num of centers 1000
    val N = args(2).toInt // max iterations 1000???
    val outfile = args(3) // file path  of final center points

    val sparkConf = new SparkConf().setAppName("SparkHuyark") //KMeans
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile(infile)
    val points = input.map {
      line =>
        {
          val arr = line.split(",")
          (arr(3).toDouble, arr(4).toDouble) // TODO: Correct the index
        }
    }.cache()

    val KMeansKernel: ((Array[(Double, Double)], Double, Boolean), Int) => (Array[(Double, Double)], Double, Boolean) = {
      case ((centers, previousDistance, isEnd), iter) =>
        {
          if (isEnd) {
            (centers, previousDistance, isEnd)
          } else {
          
            
//            val sumDistance = previousDistance
//            val newCenters = centers
            // TODO : Group points by its nearest center. From each group, calculate a new center. 
            //

            println("iter = " + iter.toString() + "\tsum distance = " + sumDistance.toString())

            (newCenters, sumDistance, previousDistance == sumDistance)
          }
        }
    }

    val minx = points.map(p => p._1).min()
    val maxx = points.map(p => p._1).max() // TODO : find the border of points
    val miny = points.map(p => p._2).min() 
    val maxy = points.map(p => p._2).max()
    
//   Array.fi
    val r = scala.util.Random
    val randomCenters: Array[(Double, Double)] = Array.fill(K)((r.nextDouble()*(maxx-minx) + minx,r.nextDouble()*(maxy-miny) + miny))// TODO : random K centers

    val (finalCenters, sumDistance, isEnd) = Range(0, N).foldLeft((randomCenters, Double.MaxValue, false))(KMeansKernel)

    sc.parallelize(finalCenters).saveAsTextFile(outfile)
  }

}
