package database.manager

import java.sql.ResultSet
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object wrapper {

    implicit class WrappedResultSet(val rs: ResultSet) extends AnyVal {

        def getInt(index: Int): Int = rs.getInt(index)
        def getInt(column: String): Int = rs.getInt(column)

        def getLong(index: Int): Long = rs.getLong(index)
        def getLong(column: String): Long = rs.getLong(column)

        def getDouble(index: Int): Double = rs.getDouble(index)
        def getDouble(column: String): Double = rs.getDouble(column)

        def getString(index: Int): String = rs.getString(index)
        def getString(column: String): String = rs.getString(column)

        def getJodaTime(index: Int): DateTime = {
            val timeCode: String = rs.getString(index)
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(timeCode)
        }
        def getJodaTime(column: String): DateTime = {
            val timeCode: String = rs.getString(column)
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(timeCode)
        }

    }

    implicit class EscapeSQL(val sc: StringContext) extends AnyVal {

        def sql(args: Any*): String = {
            val strings: Iterator[String] = sc.parts.iterator
            val expressions: Iterator[Any] = args.iterator
            var buffer: StringBuilder = new StringBuilder(strings.next)
            while (strings.hasNext) {
                buffer.append(expressions.next match {
                    case x: DateTime => "\'" + x.toString("yyyy-MM-dd HH:mm:ss") + "\'"
                    case x => "\'" + x.toString + "\'"
                })
                buffer.append(strings.next)
            }
            buffer.toString
        }

    }

}
