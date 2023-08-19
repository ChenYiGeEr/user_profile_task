package com.lim.userprofile.app

import com.lim.userprofile.bean.TagInfo
import com.lim.userprofile.constants.ConstCode
import com.lim.userprofile.dao.TagInfoDAO
import com.lim.userprofile.utils.{PropertiesUtils, SqlUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object TagMergeApp {

  /** 创建宽表 */
  def main(args: Array[String]): Unit = {

    // 1. 请求参数判断
    if (args.length != 2) {
      println("Usage: TagMergeApp <taskId> <taskDate>")
      System.exit(1)
    }

    // 第一个参数 会传入任务编号
    val taskId: String = args.head
    // 第二个参数 会传入任务的业务日期
    val taskDate: String = args(1)
    // 1 查询所有开启的标签任务
    val tagInfoList: List[TagInfo] = TagInfoDAO.getTagListOnTask()
    // 1.1 若没有开启的标签任务，直接退出
    if (tagInfoList.isEmpty) {
      println("Usage: No TagInfoList")
      System.exit(1)
    }
    // 2 环境
    // 2.1 Spark环境
    val sparkConf: SparkConf = new SparkConf()
                                .setAppName("tag_merge_app")
//                              .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 2.2 加载数据库名的配置
    val properties: Properties = PropertiesUtils.load("config.properties")
    val userProfileDbName = properties.getProperty("user-profile.dbname")
    // val wareHouseDbName = properties.getProperty("data-warehouse.dbname")
    val hdfsStorePath = properties.getProperty("hdfs-store.path")
    // 3 通过TagCode 创建当日新表
    // 3.1 自定义表名 每日产生新表
    val mergeTableName: String = s"${ConstCode.TABLE_NAME_PREFIX}${taskDate.replaceAll("-","")}"
    val dropTableSQL: String = SqlUtils.genDropTableSQL(mergeTableName)
    val createTableSql: String = SqlUtils.genCreateTableSQLNoPartition(tagInfoList: List[TagInfo],
                                                                        mergeTableName,
                                                          s"标签宽表${taskDate.replaceAll("-","")}",
                                                                        userProfileDbName,
                                                                        hdfsStorePath)
    // 4 拼接 insert select语句
    val insertTableSql: String = genInsertTableSql(tagInfoList: List[TagInfo],
                                                    mergeTableName,
                                                    userProfileDbName,
                                                    taskDate)
    println(s" use ${userProfileDbName}")
    println(dropTableSQL)
    println(createTableSql)
    println(insertTableSql)
    // 5 sparkSession执行插入宽表和数据的sql
    sparkSession.sql(s" use ${userProfileDbName}")
    sparkSession.sql(dropTableSQL)
    sparkSession.sql(createTableSql)
    sparkSession.sql(insertTableSql)
  }

  /**
   * 生成insert语句
   * @param tagInfoList 标签信息列表
   * @param tableName 标签表名
   * @param taskDate 任务日期
   * @return insert语句
   * */
  def genInsertTableSql(tagInfoList: List[TagInfo],
                        tableName: String,
                        dbName: String,
                        taskDate: String): String = {
    // 将所有标签表的查询语句拼接成一个大的union all
    val tagsSql: String = tagInfoList
      .map(tagInfo => {
        // select uid, tag_value, 'tag_person_nature_age' tag_name from tag_person_nature_age where dt = '2021-07-01'
        "select uid, tag_value,'" + tagInfo.tagCode + "' tag_name from " + tagInfo.tagCode.toLowerCase + s" where dt = '${taskDate}'"
      })
      .mkString(" union all ")

    // insert 语句
    val insertSql = s"insert overwrite table ${dbName}.${tableName}"
    // 查询语句
    val selectSql = " select * from (" + tagsSql + ") " +
      s"tags pivot ( concat_ws(',',collect_list(tag_value)) as v for tag_name in (${tagInfoList.map("'" + _.tagCode + "'").mkString(",")}));"
    insertSql + selectSql
  }
}