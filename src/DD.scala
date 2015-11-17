/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD
import java.util.ArrayList
import java.util.List
//remove if not needed
import scala.collection.JavaConversions._
import scala.util.control.Breaks._


class DD[T] {

  def split(inputRDD: JavaRDD[T], numberOfPartitions: Int, splitFunc: userSplit[T]): Array[JavaRDD[T]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions)
  }

  def test(inputRDD: JavaRDD[T], testFunc: userTest[T]): Boolean = testFunc.usrTest(inputRDD)

  private def dd_helper(inputRDD: JavaRDD[T],
                        numberOfPartitions: Int,
                        testFunc: userTest[T],
                        splitFunc: userSplit[T]) {
    var rdd = inputRDD
    var partitions = numberOfPartitions
    var runTime = 1
    var bar_offset = 0
    val failing_stack = new ArrayList[SubRDD[T]]()
    failing_stack.add(0, new SubRDD[T](rdd, partitions, bar_offset))
    while (!failing_stack.isEmpty) {
      breakable {
        val subrdd = failing_stack.remove(0)
        rdd = subrdd.rdd
        bar_offset = subrdd.bar
        partitions = subrdd.partition
        val assertResult = test(rdd, testFunc)
        if (!assertResult) break
        if (rdd.count() <= 1) {
          println("DD: Done, RDD only holds one line")
          println("Delta Debugged Error inducing inputs: " + rdd.collect())
          break
        }
        println("Spliting now...")
        val rddList = split(rdd, partitions, splitFunc)
        println("Splitting to " + partitions + " partitions is done.")
        var rdd_failed = false
        var rddBar_failed = false
        var next_rdd = rdd
        var next_partitions = partitions
        for (i <- 0 until partitions) {
            println("Generating subRDD id:" + rddList(i).id + " with line counts: " +
            rddList(i).count())
        }
        for (i <- 0 until partitions) {
          println("Testing subRDD id:" + rddList(i).id)
          val result = test(rddList(i), testFunc)
          println("Testing is done")
          if (result) {
            rdd_failed = true
            next_partitions = 2
            bar_offset = 0
            failing_stack.add(0, new SubRDD(rddList(i), next_partitions, bar_offset))
          }
        }
        if (!rdd_failed) {
          for (j <- 0 until partitions) {
            val i = (j + bar_offset) % partitions
            val rddBar = rdd.subtract(rddList(i))
            val result = test(rddBar, testFunc)
            if (result) {
              rddBar_failed = true
              next_rdd = next_rdd.intersection(rddBar)
              next_partitions = next_partitions - 1
              bar_offset = i
              failing_stack.add(0, new SubRDD[T](next_rdd, next_partitions, bar_offset))
            }
          }
        }
        if (!rdd_failed && !rddBar_failed) {
          if (rdd.count() <= 2) {
            println("DD: Done, RDD only holds one line")
            println("Delta Debugged Error inducing inputs: " + rdd.collect())
            break
          }
          next_partitions = Math.min(rdd.count().toInt, partitions * 2)
          failing_stack.add(0, new SubRDD[T](rdd, next_partitions, bar_offset))
          println("DD: Increase granularity to: " + next_partitions)
        }
        partitions = next_partitions
        runTime = runTime + 1
        println("Finish one loop of dd")
      }
    }
  }

  def ddgen(inputRDD: JavaRDD[T], testFunc: userTest[T], splitFunc: userSplit[T]) {
    dd_helper(inputRDD, 2, testFunc, splitFunc)
  }
}

class SubRDD[T](var rdd: JavaRDD[T], var partition: Int, var bar: Int)


