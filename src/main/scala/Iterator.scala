package database.manager

import scala.reflect._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.sql.ResultSetMetaData

sealed abstract class DBIterator[E: ClassTag, TT](
    val Factory: SQLEntity[E],
    val primaryKeyColumn: String,
    val chunkSize: Int,
    val validation: Boolean,
    private val database: String,
    private val conn: Connection
) extends Iterator[TT] {

    require( check )

    protected var running: Boolean = true
    protected var cursor: Int = 0
    protected var log: List[Long] = List.empty[Long]
    protected val max: Int = count()
    protected val columns: String = implicitly[ClassTag[E]].runtimeClass.getDeclaredFields.map(_.getName).mkString(", ")
    protected val conditions: String = s" order by ${primaryKeyColumn} limit ${chunkSize} offset "

    def next(): TT

    def hasNext(): Boolean = {
        running = if (cursor <= max) true else false
        running
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
        log ::= rs.getLong(primaryKeyColumn)
        assert(log.length == log.distinct.length, "Error [Arugument Column Invalid]:: primary key column not unique.")
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

    def currentPosition(): Map[String, Int] = {
        Map("current" -> Math.floor(cursor / chunkSize).toInt, "max" -> Math.floor(max / chunkSize).toInt)
    }

}

class DBOnceIterator[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, chunkSize: Int, validation: Boolean)(database: String, conn: Connection)
        extends DBIterator[T, T](Factory, primaryKeyColumn, chunkSize, validation, database, conn) {

    def next(): T = {
        running match {
            case true => cursor += chunkSize
            case false => Unit
        }
        val st: Statement = conn.createStatement
        val result: T = try {
            val sqlContext: String = "select " + columns + " from " + Factory.table + conditions + cursor.toString
            val rs: ResultSet = st.executeQuery(sqlContext)
            validation match {
                case true => Iterator.continually(rs).takeWhile(_.next).map(rs => validator(rs)).toList(0)
                case false => Iterator.continually(rs).takeWhile(_.next).map(rs => Factory(rs)).toList(0)
            }
        } finally {
            st.close()
        }
        result
    }

}

class DBGroupedIterator[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, chunkSize: Int, validation: Boolean)(database: String, conn: Connection )
        extends DBIterator[T, List[T]](Factory, primaryKeyColumn, chunkSize, validation, database, conn) {

    def next(): List[T] = {
        running match {
            case true => cursor += chunkSize
            case false => Unit
        }
        val st: Statement = conn.createStatement
        val result: List[T] = try {
            val sqlContext: String = "select " + columns + " from " + Factory.table + conditions + cursor.toString
            val rs: ResultSet = st.executeQuery(sqlContext)
            validation match {
                case true => Iterator.continually(rs).takeWhile(_.next).map(rs => validator(rs)).toList
                case false => Iterator.continually(rs).takeWhile(_.next).map(rs => Factory(rs)).toList
            }
        } finally {
            st.close()
        }
        result
    }

}
