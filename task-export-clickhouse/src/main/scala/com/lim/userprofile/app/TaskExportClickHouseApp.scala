package com.lim.userprofile.app

import com.lim.userprofile.bean.TagInfo
import com.lim.userprofile.constants.ConstCode
import com.lim.userprofile.dao.TagInfoDAO
import com.lim.userprofile.utils.{ClickhouseUtils, PropertiesUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession}

import java.util.Properties

object TaskExportClickHouseApp{

  /** 将标签宽表平移到clickhouse中 */
  def main(args: Array[String]): Unit = {
    // 0 参数校验
    if (args.length != 2) {
      println("Usage: TaskExportClickHouseApp <taskId> <taskDate>")
      System.exit(1)
    }
    // 第一个参数 会传入任务编号
    val taskId: String = args.head
    // 第二个参数 会传入任务的业务日期
    val taskDate: String = args(1)
    // 1 建表 每天建一个表
    // 1.1 查询所有开启的标签任务
    val tagInfoList: List[TagInfo] = TagInfoDAO.getTagListOnTask()
    // 1.2 若没有开启的标签任务，直接退出
    if (tagInfoList.isEmpty) {
      println("Usage: No TagInfoList")
      System.exit(1)
    }
    val tagColNames: String = tagInfoList.map(_.tagCode.toLowerCase + " String ").mkString(",")
    // hive和clickhouse的表名(不能以数字开头)
    val tableName: String = s"${ConstCode.TABLE_NAME_PREFIX}${taskDate.replaceAll("-","")}"
    val createTableClickHouseSQL =
      s"""
         |CREATE TABLE IF NOT EXISTS ${tableName}
         |(
         |  uid Int64,
         |  ${tagColNames}
         |)
         | engine = MergeTree
         | order by uid
         |""".stripMargin
    // 1.3 在clickhouse中删表后建表
    ClickhouseUtils.executeSql(s"DROP TABLE IF EXISTS ${tableName}");
    ClickhouseUtils.executeSql(createTableClickHouseSQL);
    // 2 查询hive中的数据
    // 2.1 Spark环境
    val sparkConf: SparkConf = new SparkConf().setAppName("tag_merge_app")
    // .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 2.2 Properties 配置
    val properties: Properties = PropertiesUtils.load("config.properties")
    val userProfileDbName = properties.getProperty("user-profile.dbname")
    val clickhouseUrl = properties.getProperty("clickhouse.url")
    // 3 读取要插入的数据
    // 4 spark通过jdbc将hive表中数据写入clickhouse数据库中
    sparkSession
      .sql(s"select * from $userProfileDbName.$tableName")
      .write
      .mode(SaveMode.Append)
      .option("batchsize", "100") // 批量写入的批量个数
      .option("isolationLevel", "NONE") // 关闭事务
      .option("numPartitions", "4") // 设置并发
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .jdbc(clickhouseUrl, tableName, new Properties())
  }

}