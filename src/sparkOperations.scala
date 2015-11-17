import java.util.StringTokenizer

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.api.java.function.{Function2, PairFunction}

import scala.util.control.Breaks._

/**
 * Created by Michael on 11/12/15.
 */
class sparkOperations {
  def sparkWorks(text :JavaRDD[String]): JavaPairRDD[Float, Integer] = {
    val division = 0.5f
    val averageRating = text.mapToPair(new PairFunction[String, Float, Integer]() {

      override def call(s: String): Tuple2[Float, Integer] = {
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
        val result = new Tuple2[Float, Integer](outValue, 1)
        return result
      }
    })
    val counts = averageRating.reduceByKey(new Function2[Integer, Integer, Integer]() {

      override def call(integer: java.lang.Integer, integer2: java.lang.Integer): java.lang.Integer = {
        return integer + integer2
      }
    })
    counts
  }
}
