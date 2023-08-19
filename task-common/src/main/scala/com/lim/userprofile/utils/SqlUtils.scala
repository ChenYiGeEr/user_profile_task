package com.lim.userprofile.utils

import java.lang.reflect.Field
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties
import com.alibaba.fastjson.JSONObject
import com.google.common.base.CaseFormat
import com.lim.userprofile.bean.TagInfo

import scala.collection.mutable.ListBuffer

object SqlUtils {

  private val properties: Properties = PropertiesUtils.load("config.properties")

  val MYSQL_URL = properties.getProperty("mysql.url")
  val MYSQL_USERNAME = properties.getProperty("mysql.username")
  val MYSQL_PASSWORD = properties.getProperty("mysql.password")

  /**
   * genDropTableSQL
   * <p>生成删除表的SQL</p>
   *
   * @param tableName 表名
   * @return 删除表的sql语句
   * */
  def genDropTableSQL(tableName: String): String = { s"drop table if exists $tableName;" }

  /**
   * genCreateTableSQLNoPartition
   * <p>生成创建没有分区的表的SQL</p>
   *
   * @param tagInfoList 标签信息列表
   * @param tableName 表名
   * @param tableComment 表注释
   * @param dbName 数据库名称
   * @param hdfsStorePath hive表存储在hdfs上的路径
   * @return 创建表的sql语句
   *  */
  def genCreateTableSQLNoPartition(tagInfoList: List[TagInfo],
                                   tableName :String,
                                   tableComment: String,
                                   dbName: String,
                                   hdfsStorePath:String ): String = {
    val columnList: List[String] = tagInfoList.map(_.tagCode.toLowerCase + "  string")
    val columnListSql: String =  "uid string ," + columnList.mkString(",")
    s"""
       | create table ${tableName} ( ${columnListSql} )
       | comment ${tableComment}
       | ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
       | LOCATION '${hdfsStorePath}/${dbName}/${tableName.toLowerCase}/';""".stripMargin
  }

  def queryList(sql: String): java.util.List[JSONObject] = {
    Class.forName("com.mysql.jdbc.Driver")
    //创建结果列表
    val resultList: java.util.List[JSONObject] = new java.util.ArrayList[JSONObject]()
    //创建连接
    val conn: Connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)
    //创建会话
    val stat: Statement = conn.createStatement
    //提交sql 返回结果
    val rs: ResultSet = stat.executeQuery(sql)
    //为了获得列名 要取得元数据
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        // 根据下标得到对应元数据中的字段名，以及结果中值
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList.add(rowData)
    }

    stat.close()
    conn.close()
    resultList
  }


  def queryList[T<:AnyRef](sql: String, clazz: Class[T], underScoreToCamel: Boolean):List[T]={

    Class.forName("com.mysql.jdbc.Driver");
    val resultList: ListBuffer[T] = new ListBuffer[T]();
    val connection: Connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)
    val stat: Statement = connection.createStatement();
    val rs: ResultSet = stat.executeQuery(sql);
    val md: ResultSetMetaData = rs.getMetaData();
    while (rs.next()) {
      val obj: T = clazz.newInstance()
      for (i <- 1 to md.getColumnCount) {
        var propertyName: String = md.getColumnLabel(i)

        if (underScoreToCamel) {
          propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnLabel(i))
        }
        BeanUtils.setV(obj,propertyName,rs.getObject(i))

      }
      resultList.append(obj);
    }
    stat.close()
    connection.close()
    resultList.toList

  }

  def queryOne[T<:AnyRef](sql: String, clazz: Class[T] ,
                          underScoreToCamel: Boolean): Option[T ] ={
    Class.forName("com.mysql.jdbc.Driver");
    val resultList: ListBuffer[T] = new ListBuffer[T]();
    val connection: Connection =
      DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)
    val stat: Statement = connection.createStatement();
    val rs: ResultSet = stat.executeQuery(sql);
    val md: ResultSetMetaData = rs.getMetaData();
    while (rs.next()) {
      val obj: T = clazz.newInstance()


      for (i <- 1 to md.getColumnCount) {
        var propertyName: String = md.getColumnName(i)
        if (underScoreToCamel) {
          propertyName =
            CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnName(i))
        }
        BeanUtils.setV(obj,propertyName,rs.getObject(i))
      }
      resultList.append(obj);
    }
    stat.close()
    connection.close()
    resultList.headOption

  }

  def insertOne[T](sql: String, obj:T ): Unit ={
    Class.forName("com.mysql.jdbc.Driver");
    val resultList: ListBuffer[T] = new ListBuffer[T]();
    val connection: Connection =
      DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD)
    val pstat: PreparedStatement = connection.prepareStatement(sql)
    val fields: Array[Field] = obj.getClass.getDeclaredFields

    for(i<- 1 to fields.size){
      val field:Field = fields(i-1);
      field.setAccessible(true)
      val value: AnyRef = field.get(obj)
      pstat.setObject(i,value)
    }
    val resInsert = pstat.execute()
    println("插入数据，返回值=>" + resInsert)

  }

}
