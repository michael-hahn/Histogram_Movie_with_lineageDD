/**
 * Created by Michael on 11/12/15.
 */
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
//remove if not needed
import scala.collection.JavaConversions._

class Review extends Writable {

  var rater_id: Int = -1

  var rating: Int = 0

  def this(a: Review) {
    this()
    rater_id = a.rater_id
    rating = a.rating
  }

  def clear() {
    rater_id = -1
    rating = 0
  }

  def readFields(in: DataInput) {
    rater_id = in.readInt()
    rating = in.readInt()
  }

  def write(out: DataOutput) {
    out.writeInt(rater_id)
    out.writeInt(rating)
  }
}

