/**
 * Created by Michael on 11/12/15.
 */
import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util._
//remove if not needed
import scala.collection.JavaConversions._

class Cluster extends Writable {

  var clusterID: Int = -1

  var movie_id: Long = -1

  var total: Int = 0

  var similarity: Float = 0.0f

  var reviews: ArrayList[Review] = new ArrayList[Review]()

  def this(a: Cluster) {
    this()
    clusterID = a.clusterID
    movie_id = a.movie_id
    total = a.total
    similarity = a.similarity
    reviews = new ArrayList[Review]()
    var rv: Review = null
    for (i <- 0 until a.reviews.size) {
      rv = new Review(a.reviews.get(i))
      reviews.add(rv)
    }
  }

  def readFields(in: DataInput) {
    clusterID = in.readInt()
    movie_id = in.readLong()
    total = in.readInt()
    similarity = in.readFloat()
    reviews.clear()
    val size = in.readInt()
    var rs: Review = null
    for (i <- 0 until size) {
      rs = new Review()
      rs.rater_id = in.readInt()
      rs.rating = in.readByte()
      reviews.add(rs)
    }
  }

  def write(out: DataOutput) {
    out.writeInt(clusterID)
    out.writeLong(movie_id)
    out.writeInt(total)
    out.writeFloat(similarity)
    val size = reviews.size
    out.writeInt(size)
    for (i <- 0 until size) {
      out.writeInt(reviews.get(i).rater_id)
      out.writeByte(reviews.get(i).rating)
    }
  }
}
