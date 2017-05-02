package database.manager

import scala.reflect._
import org.joda.time._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import wrapper.WrappedResultSet
import wrapper.EscapeSQL

trait SQLEntity[E] {
    val table: String
    def apply(rs: WrappedResultSet): E
    def unapply(e: E): Option[_ <: Product]
}

class DBConnection( val database: String, val username: String, val password: String ) {

    private val conn: Connection = DriverManager.getConnection(s"jdbc:postgresql://127.0.0.1:5432/${database}", username, password)

    def execute(sqlContext: String, logging: Boolean = false): Unit = {
        val st: Statement = conn.createStatement
        try {
            st.execute(sqlContext)
        } finally {
            st.close()
            log("EXECUTE : " + sqlContext, logging)
        }
    }

    def delete(target: String, logging: Boolean = false): Unit = {
        val st: Statement = conn.createStatement
        try {
            val sqlContext: String = "delete from " + target
            st.executeUpdate(sqlContext)
        } finally {
            st.close()
            log("DELETE : " + target, logging)
        }

    }

    def extract(sqlContext: String, logging: Boolean = false): List[Map[String,Any]] = {
        val st: Statement = conn.createStatement
        val resultSet:List[Map[String,Any]] = try {
            val rs: ResultSet = st.executeQuery(sqlContext)
            val md: ResultSetMetaData = rs.getMetaData()
            val buildMap = () => (1 to md.getColumnCount).map(n => md.getColumnName(n) -> rs.getObject(n)).toMap
            Iterator.continually(rs).takeWhile(_.next).map(_ => buildMap()).toList
        } finally {
            st.close()
        }
        log("SELECT : " + resultSet, logging)
        resultSet
    }

    def deploy[T: ClassTag](Factory: SQLEntity[T], limit: Int = 0, logging: Boolean = false): List[T] = {
        val cs: Array[String] = implicitly[ClassTag[T]].runtimeClass.getDeclaredFields.map(_.getName)
        val st: Statement = conn.createStatement
        val resultSet:List[T] = try {
            val sqlContext: String = limit match {
                case 0 => "select " + cs.mkString(", ") + " from " + Factory.table
                case x: Int  => "select " + cs.mkString(", ") + " from " + Factory.table + " limit " + x.toString
            }
            val rs: ResultSet = st.executeQuery(sqlContext)
            Iterator.continually(rs).takeWhile(_.next).map(rs => Factory(rs)).toList
        } finally {
            st.close()
        }
        log("SELECT : " + resultSet, logging)
        resultSet
    }

    def update[T: ClassTag](Factory: SQLEntity[T], target: String, logging: Boolean = false)(Store: T): Unit = {
        val cs: Array[String] = implicitly[ClassTag[T]].runtimeClass.getDeclaredFields.map(_.getName)
        val st: Statement = conn.createStatement()
        try {
            val vs: List[String] = Factory.unapply(Store).get.productIterator.toList.map(Option(_) match {
                case Some(x) => sql"${x}"
                case None => "null"
            })
            val sqlContext: String = "insert into " + target + cs.mkString("(", ", ", ")") + " values " + vs.mkString("(", ", ", ")")
            st.executeUpdate(sqlContext)
            log("INSERT : " + vs.mkString("(", ", ", ")"), logging)
        } finally {
            st.close()
        }
    }

    def copy[T: ClassTag](Factory: SQLEntity[T], target: String, logging: Boolean = false): Unit = {
        val st: Statement = conn.createStatement
        try {
            val sqlContext: String = "create table " + target + " ( like " + Factory.table + " including indexes )"
            st.executeUpdate(sqlContext)
        } finally {
            st.close()
            log("COPY TABLE : " + target, logging)
        }
    }

    def iterateOnce[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, validation: Boolean = false): DBIterator[T] = new DBOnceIterator(Factory, primaryKeyColumn, 1, validation)(database, conn)

    def iterateGroup[T: ClassTag](Factory: SQLEntity[T], primaryKeyColumn: String, chunkSize: Int = 1, validation: Boolean = false): DBIterator[List[T]] = new DBGroupedIterator(Factory, primaryKeyColumn, chunkSize, validation)(database, conn)

    def log(logContext: String, logging: Boolean): Unit = logging match {
        case true => println(logContext)
        case false => Unit
    }

    def release(): Unit = {
        conn.close()
    }

}
