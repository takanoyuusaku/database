package database.manager

import scala.reflect._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.sql.ResultSetMetaData

sealed trait DBIterator[TT] extends Iterator[TT]{
    protected var running: Boolean
    protected var cursor: Int
    protected var history: List[Long]
    protected val max: Int
    protected val columns: String
    protected val conditions: String

    def info(): Map[String, Any]
}

sealed abstract class DBAbstractIterator[E: ClassTag, TT](
    val Factory: SQLEntity[E],
    val primaryKeyColumn: String,
    val chunkSize: Int,
    val validation: Boolean,
    private val database: String,
    private val conn: Connection
) extends DBIterator[TT] {

    require( check )

    override protected var running: Boolean = true
    override protected var cursor: Int = 0
    override protected var history: List[Long] = List.empty[Long]
    override protected val max: Int = count()
    override protected val columns: String = implicitly[ClassTag[E]].runtimeClass.getDeclaredFields.map(_.getName).mkString(", ")
    override protected val conditions: String = s" order by ${primaryKeyColumn} limit ${chunkSize} offset "

    override def hasNext(): Boolean = {
        running = if (cursor <= max) true else false
        running
    }

    override def info(): Map[String, Any] = {
        Map(
            "current" -> Math.floor(cursor / chunkSize).toInt,
            "max" -> Math.floor(max / chunkSize).toInt,
            "table" -> Factory.table,
            "running" -> running
        )
    }

    def count(): Int = {
        val st: Statement = conn.createStatement
        val result: Int = try {
            val sqlContext: String = "select count(*) from " + Factory.table
            val rs: ResultSet = st.executeQuery(sqlContext)
            Iterator.continually(rs).takeWhile(_.next).map(rs => rs.getInt(1)).toList(0)
        } finally {
            st.close()
        }
        result
    }

    def validator(rs: ResultSet): E = {
        history ::= rs.getLong(primaryKeyColumn)
        assert(history.length == history.distinct.length, "Error [Arugument Column Invalid]:: primary key column not unique.")
        Factory(rs)
    }

    def check(): Boolean = {
        val st: Statement = conn.createStatement
        val result: String = try {
            val sqlContext: String = s"select ccu.column_name as COLUMN_NAME from information_schema.table_constraints tc, information_schema.constraint_column_usage ccu where tc.table_catalog='${database}' and tc.table_name='${Factory.table}' and tc.constraint_type='PRIMARY KEY' and tc.table_catalog=ccu.table_catalog and tc.table_schema=ccu.table_schema and tc.table_name=ccu.table_name and tc.constraint_name=ccu.constraint_name"
            val rs: ResultSet = st.executeQuery(sqlContext)
            Iterator.continually(rs).takeWhile(_.next).map(rs => rs.getString(1)).toList(0)
        } finally {
            st.close()
        }
        result equalsIgnoreCase primaryKeyColumn
    }

}

class DBOnceIterator[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, chunkSize: Int, validation: Boolean)(database: String, conn: Connection)
        extends DBAbstractIterator[T, T](Factory, primaryKeyColumn, chunkSize, validation, database, conn) {

    override def next(): T = {
        running match {
            case true => cursor += chunkSize
            case false => Unit
        }
        val st: Statement = conn.createStatement
        val result: T = try {
            val sqlContext: String = "select " + columns + " from " + Factory.table + conditions + cursor.toString
            val rs: ResultSet = st.executeQuery(sqlContext)
            validation match {
                case true =>
                    println("Current Access -> " + Factory.table + " :: " +  Math.floor(cursor / chunkSize).toInt + "/" + Math.floor(max / chunkSize).toInt)
                    Iterator.continually(rs).takeWhile(_.next).map(rs => validator(rs)).next
                case false => Iterator.continually(rs).takeWhile(_.next).map(rs => Factory(rs)).next
            }
        } finally {
            st.close()
        }
        result
    }

}

class DBGroupedIterator[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, chunkSize: Int, validation: Boolean)(database: String, conn: Connection )
        extends DBAbstractIterator[T, List[T]](Factory, primaryKeyColumn, chunkSize, validation, database, conn) {

    override def next(): List[T] = {
        running match {
            case true => cursor += chunkSize
            case false => Unit
        }
        val st: Statement = conn.createStatement
        val result: List[T] = try {
            val sqlContext: String = "select " + columns + " from " + Factory.table + conditions + cursor.toString
            val rs: ResultSet = st.executeQuery(sqlContext)
            validation match {
                case true =>
                    println("Current Access -> " + Factory.table + " :: " +  Math.floor(cursor / chunkSize).toInt + "/" + Math.floor(max / chunkSize).toInt)
                    Iterator.continually(rs).takeWhile(_.next).map(rs => validator(rs)).toList
                case false => Iterator.continually(rs).takeWhile(_.next).map(rs => Factory(rs)).toList
            }
        } finally {
            st.close()
        }
        result
    }

}
