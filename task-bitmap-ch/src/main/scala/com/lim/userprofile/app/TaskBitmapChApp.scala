package com.lim.userprofile.app

import com.lim.userprofile.bean.TagInfo
import com.lim.userprofile.constants.ConstCode
import com.lim.userprofile.constants.ConstCode.{TAG_VALUE_TYPE_DATE, TAG_VALUE_TYPE_DECIMAL, TAG_VALUE_TYPE_LONG, TAG_VALUE_TYPE_STRING}
import com.lim.userprofile.dao.TagInfoDAO
import com.lim.userprofile.utils.{ClickhouseUtils, PropertiesUtils}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

object TaskBitmapChApp {

  /**
   * 将clickhouse中的宽表转换成位图
   * @param args 程序运行时传入的参数
   * */
  def main(args: Array[String]): Unit = {
    // 1. 请求参数判断
    if (args.length != 2) {
      println("Usage: TaskBitmapChApp <taskId> <taskDate>")
      System.exit(1)
    }
    // 第一个参数 会传入任务编号
    val taskId: String = args.head
    // 第二个参数 会传入任务的业务日期
    val taskDate: String = args.last.replace("-", "")
    // 2. 环境
    // 2.1 clickhouse中的用户画像数据库名
    val tagInfoList: List[TagInfo] = TagInfoDAO.getTagListOnTask()
    // 2.2 若没有开启的标签任务，直接退出
    if (tagInfoList.isEmpty) {
      println("Usage: No TagInfoList")
      System.exit(1)
    }
    // 2.3 Spark环境
    val sparkConf: SparkConf = new SparkConf()
                                .setAppName("tag_bitmap_ch_app")
//                                .setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    // 2.4 加载配置文件
    val properties: Properties = PropertiesUtils.load("config.properties")
    // 2.5 hive中的用户画像数据库库名
    val userProfileClickhouseDatabaseName:String = properties.getProperty("clickhouse.url").split("/").last
    // 2.6 hive中的宽表表名
    val withTableName = s"${ConstCode.TABLE_NAME_PREFIX}${taskDate}"
    val tuples = getTableAndTagInfoList(tagInfoList)
    // 2.7 循环遍历执行插入sql
    for (tuples <- tuples) {
      // 2.7.1 清空数据
      ClickhouseUtils.executeSql(s"alter table $userProfileClickhouseDatabaseName.${tuples._1} delete where dt = '$taskDate';")
      // 2.7.2 判断集合不为空时 执行sql语句
      if (tuples._2.nonEmpty) {
        // 2.7.3 插入数据
        ClickhouseUtils.executeSql(genInsertSQLByTagType(userProfileClickhouseDatabaseName, tuples._1, tuples._2, withTableName, taskDate))
      }
    }
  }

  def getTableAndTagInfoList(tagInfoList: List[TagInfo]) : List[(String, List[TagInfo])] = {
    tagInfoList.groupBy(_.tagValueType).map{
      case (tagValueType, tagInfoList) => {
        tagValueType match {
          case TAG_VALUE_TYPE_LONG => (ConstCode.CLICKHOUSE_BITMAP_TAG_TABLE_PREFIX + "long", tagInfoList.filter(_.tagValueType == TAG_VALUE_TYPE_LONG))
          case TAG_VALUE_TYPE_DECIMAL => (ConstCode.CLICKHOUSE_BITMAP_TAG_TABLE_PREFIX + "decimal", tagInfoList.filter(_.tagValueType == TAG_VALUE_TYPE_DECIMAL))
          case TAG_VALUE_TYPE_STRING => (ConstCode.CLICKHOUSE_BITMAP_TAG_TABLE_PREFIX + "string", tagInfoList.filter(_.tagValueType == TAG_VALUE_TYPE_STRING))
          case TAG_VALUE_TYPE_DATE => (ConstCode.CLICKHOUSE_BITMAP_TAG_TABLE_PREFIX + "date", tagInfoList.filter(_.tagValueType == TAG_VALUE_TYPE_DATE))
        }
      }
    }.toList
  }

  /**
   * 生成插入位图表的的sql语句
   * @param dbName 数据库名称
   * @param tableName 插入的表名
   * @param tagInfoList tag信息列表
   * @param withTableName hive宽表表名
   * */
  def genInsertSQLByTagType(dbName:String,
                            tableName: String,
                            tagInfoList: List[TagInfo],
                            withTableName: String,
                            taskDate: String): String = {
    val dynamicFields = tagInfoList.map(element => {
        s"('${element.tagCode.toLowerCase}', ${element.tagCode.toLowerCase})"
    }).mkString(",")
    s"""
     | insert into $dbName.$tableName
     |    select
     |      tag_code_value.1 as tag_code,
     |      tag_code_value.2 as tag_value,
     |      groupBitmapState(uid) as us,
     |      '${taskDate}' as dt
     |        from (
     |          SELECT
     |            arrayJoin([$dynamicFields])
     |            AS tag_code_value,
     |            cast(uid as UInt64) uid
     |      FROM $dbName.$withTableName
     |      ) tv
     | WHERE tag_code_value.2 <> ''
     | group by
     |      tag_code_value.1,
     |      tag_code_value.2
     |""".stripMargin
  }
}
