/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD
//remove if not needed
import scala.collection.JavaConversions._

trait userTest[T] {

  def usrTest(inputRDD: JavaRDD[T]): Boolean
}
