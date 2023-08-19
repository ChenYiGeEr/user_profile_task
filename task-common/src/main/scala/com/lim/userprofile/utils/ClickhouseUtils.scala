package com.lim.userprofile.utils

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object ClickhouseUtils {

  private val properties: Properties = PropertiesUtils.load("config.properties")
  val CLICKHOUSE_URL = properties.getProperty("clickhouse.url")


  def executeSql(sql: String): Unit = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
    val connection: Connection = DriverManager.getConnection(CLICKHOUSE_URL, null, null)
    val statement: Statement = connection.createStatement()
    statement.execute(sql)
    connection.close()
  }

}
