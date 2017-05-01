import scala.io._
import database.manager._

object Test {

    def main(args: Array[String]): Unit = {
        val config: Array[String] = StdIn.readLine().split(' ')
        config match {
            case x if x.length == 4 =>
                val conn: DBConnection = new DBConnection(x(0), x(1), x(2))
                val tests: List[Map[String, Any]] = conn.extract("select * from " + x(3) + " limit 10")
                tests.foreach(x => println(x.toString))
            case _ => Unit
        }
    }

}
