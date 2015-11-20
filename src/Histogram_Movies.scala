/**
 * Created by Michael on 11/12/15.
 */


import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function2
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.rdd.RDD
import scala.Tuple2
import java.util.Calendar
import java.util.List
import java.util.StringTokenizer
//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._


object Histogram_Movies {

  private val division = 0.5f
  private val exhaustive = 1

  def mapFunc(str: String): (Float, Int) = {
    val token = new StringTokenizer(str)
    val bin = token.nextToken().toFloat
    val value = token.nextToken().toInt
    return (bin, value)
  }

  def main(args: Array[String]) {
    try {
      val sparkConf = new SparkConf().setMaster("local[8]")


      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)

      lm.addLogger(logger)
      logger.setLevel(Level.INFO)

      logger.addHandler(fh)

      //Lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }
      //

      sparkConf.setAppName("Histogram_Movies_LineageDD-" + lineage + "-" + logFile)
      .set("spark.executor.memory", "2g")


      val ctx = new JavaSparkContext(sparkConf)

      //lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //

      val startTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val startTime = System.nanoTime()

      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/HistogramMovies.jar", "org.apache.hadoop.examples.HistogramMovies", "/Users/Michael/IdeaProjects/Classification/file1s", "output").!!


      val lines = lc.textFile("../Classification/file1s", 1)

      //Compute once first to compare to the groundTruth to trace the lineage
      val averageRating = lines.map[(Float, Integer)]{s => {
          var rating: Int = 0
          var movieIndex: Int = 0
          var reviewIndex: Int = 0
          var totalReviews = 0
          var sumRatings = 0
          var avgReview = 0.0f
          var absReview: Float = 0.0f
          var fraction: Float = 0.0f
          var outValue = 0.0f
          var reviews = new String()
          //var line = new String()
          var tok = new String()
          var ratingStr = new String()
          movieIndex = s.indexOf(":")
          if (movieIndex > 0) {
            reviews = s.substring(movieIndex + 1)
            val token = new StringTokenizer(reviews, ",")
            while (token.hasMoreTokens()) {
              tok = token.nextToken()
              reviewIndex = tok.indexOf("_")
              ratingStr = tok.substring(reviewIndex + 1)
              rating = java.lang.Integer.parseInt(ratingStr)
              sumRatings += rating
              totalReviews += 1
            }
            avgReview = sumRatings.toFloat / totalReviews.toFloat
            absReview = Math.floor(avgReview.toDouble).toFloat
            fraction = avgReview - absReview
            val limitInt = Math.round(1.0f / division)

            breakable {
              for (i <- 1 to limitInt) {
                if (fraction <= (division * i)) {
                  outValue = absReview + division * i
                  break
                }
              }
            }
          }
          Tuple2[Float, Integer](outValue, 1)
          //val result = new Tuple2[Float, Integer](outValue, 1)
          //return result
        }
      }
      val counts = averageRating.reduceByKey(_ + _)
      //    val output = counts.collect()
      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      println(counts.count)

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/lineageResult"))

      //TO-DO: trace lineage before delta-debug
      val result = counts.testGroundTruth[Float, Int]("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/output/part-00000", mapFunc)
      var linRdd = counts.getLineage()
      linRdd.collect

      //    for (r <- result) {
      //      linRdd = linRdd.filter{ r =>
      //        result.contains(r)
      //      }
      //      linRdd = linRdd.goBackAll()
      //      linRdd.collect
      //      linRdd.show
      //      linRdd = counts.getLineage()
      //      linRdd.collect
      //    }
      linRdd = linRdd.filter {
        result(0)
      }
      linRdd = linRdd.goBackAll()
      linRdd.collect
      linRdd.show.collect().foreach(s => {
        pw.append(s.toString)
        pw.append('\n')
      })
      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/lineageResult", 1)
      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      //Delta-debugging part: UNCOMMENT when TO-DO part is done!!
      if (exhaustive == 1) {
        val delta_debug: DD[String] = new DD[String]
        delta_debug.ddgen(lineageResult, new Test,
          new Split, lm, fh)
      } else {
        val delta_debug: DD_NonEx[String] = new DD_NonEx[String]
        delta_debug.ddgen(lineageResult, new Test, new Split, lm, fh)
      }

      //Remove lineageResult file
      //    val lineageFile = new File("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/lineageResult")
      //    if (lineageFile.isDirectory) {
      //      for (list <- Option(lineageFile.listFiles()); child <- list) child.delete()
      //    }
      //    lineageFile.delete

      val endTime = System.nanoTime()
      val endTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "This job started at " + startTimestamp)
      logger.log(Level.INFO, "This job finished at " + endTimestamp)
      logger.log(Level.INFO, "The job took " + (endTime - startTime) / 1000000 + " milliseconds to finish")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      println("Job's DONE!")
      ctx.stop()
    }
  }
}
