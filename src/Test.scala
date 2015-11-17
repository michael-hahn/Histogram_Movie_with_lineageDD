/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer

import org.apache.spark.api.java.JavaRDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._





class Test extends userTest[String] {

  def usrTest(inputRDD: JavaRDD[String]): Boolean = {
    var returnValue = false;
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/Classification/file2"
    val file = new File(fileName)

    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/HistogramMovies.jar", "org.apache.hadoop.examples.HistogramMovies", fileName, "output").!!

    var truthList:Map[Float, Integer] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val bin :Float = token.nextToken().toFloat
      val number :Integer = token.nextToken().toInt
      truthList += (bin -> number)
    }


    val itr = output.iterator()
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (tupVal._1 != 0.0f) {
        val binName = tupVal._1
        val num = tupVal._2
        if (truthList.contains(binName)) {
          if (num != truthList.get(binName)){
            returnValue = true
          }
        } else returnValue = true
      }
    }

    val outputFile = new File("/Users/Michael/IdeaProjects/Histogram_Movies_LineageDD/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    return returnValue
  }
}