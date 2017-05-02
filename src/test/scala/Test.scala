import scala.io._
import org.joda.time._
import database.manager._
import database.manager.wrapper._

case class Photo(
    photoid: Long,
    takendate: DateTime,
    uploadtime: Long,
    latitude: Double,
    longitude: Double,
    owner: String,
    location: String,
    media: String,
    tags: String,
    url_sq: String,
    url_t: String,
    url_s: String,
    url_q: String,
    url_m: String,
    url_n: String,
    url_z: String,
    url_c: String,
    url_l: String,
    url_o: String
)

object Photo extends SQLEntity[Photo] {
    val table: String = "flickr"
    def apply(rs: WrappedResultSet): Photo = new Photo(
        rs.getLong("photoid"),
        rs.getJodaTime("takendate"),
        rs.getLong("uploadtime"),
        rs.getDouble("latitude"),
        rs.getDouble("longitude"),
        rs.getString("owner"),
        rs.getString("location"),
        rs.getString("media"),
        rs.getString("tags"),
        rs.getString("url_sq"),
        rs.getString("url_t"),
        rs.getString("url_s"),
        rs.getString("url_q"),
        rs.getString("url_m"),
        rs.getString("url_n"),
        rs.getString("url_z"),
        rs.getString("url_c"),
        rs.getString("url_l"),
        rs.getString("url_o")
    )
}

object Test {

    def main(args: Array[String]): Unit = {
        val config: Array[String] = StdIn.readLine().split(' ')
        config match {
            case x if x.length == 4 =>
                val conn: DBConnection = new DBConnection(x(0), x(1), x(2))
                val test: DBIterator[List[Photo]] = conn.iterateGroup(Photo, "photoid", 5, true)
                println(test.hasNext)
                println(test.next)
                println(test.info)
            case _ => Unit
        }
    }

}
