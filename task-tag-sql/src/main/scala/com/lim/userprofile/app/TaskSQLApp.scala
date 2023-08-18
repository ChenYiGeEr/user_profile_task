package com.lim.userprofile.app

import com.lim.userprofile.bean.{TagInfo, TaskInfo, TaskTagRule}
import com.lim.userprofile.dao.{TagInfoDAO, TaskInfoDAO, TaskTagRuleDAO}
import com.lim.userprofile.utils.{CommonUtils, PropertiesUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object TaskSQLApp {

  def main(args: Array[String]): Unit = {

    val properties: Properties = PropertiesUtils.load("config.properties")

    val userProfileDbName = properties.getProperty("user-profile.dbname")
    val wareHouseDbName = properties.getProperty("data-warehouse.dbname")
    val hdfsStorePath = properties.getProperty("hdfs-store.path")
    //1  初始化环境
    val sparkConf: SparkConf = new SparkConf()
                        .setAppName("tag_sql_app")
                        .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 第一个参数 会传入任务编号
    val taskId: String = args(0)
    // 第二个参数 会传入任务的业务日期
    val taskDate: String = args(1)

    // 查询任务信息
    val taskInfo: TaskInfo = TaskInfoDAO.getTaskInfo(taskId)

    // 查询标签信息
    val tagInfo: TagInfo = TagInfoDAO.getTagInfoByTaskId(taskId)

    // 查询标签规则信息
    val taskTagRuleList: List[TaskTagRule] = TaskTagRuleDAO.getTaskTagRuleListByTaskId(taskId)

    // 根据 标签名称建表  ，如果没有就建表    根据字段类型
    //用tagcode作为表名
    //根据标签的值类型来 定义字段类型
    val tableName: String = tagInfo.tagCode
    val tagValueType: String = CommonUtils.convertToHiveTableFieldType(tagInfo.tagValueType)

    val createTableSql =
      s"""create table if not exists $userProfileDbName.$tableName
         | (
         | uid string comment '用户编号',
         | tag_value $tagValueType comment '对应值'
         | ) comment '${tagInfo.tagName}'
         | PARTITIONED BY (`dt` STRING)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         | LOCATION '$hdfsStorePath/$userProfileDbName/${tableName.toLowerCase}/';""".stripMargin

    //    sparkSession.sql("select * from gmall2021.dwd_order_info ").show(20)
    sparkSession.sql(s" use ${userProfileDbName}")
    sparkSession.sql(createTableSql)

    //提取任务sql
    //处理时间
    var taskSql: String = taskInfo.taskSql
                  .replace("$dt", taskDate)
                  .replace("${dt}", taskDate)

    //组合SQL
    val insertSqlBuilder: StringBuilder =
      new StringBuilder(s"insert overwrite table ${userProfileDbName}.${tableName.toLowerCase} partition (dt='" + taskDate + "' )")
    val selectSqlBuilder: StringBuilder = new StringBuilder(" select uid, ")
    if (taskTagRuleList.nonEmpty) {
      selectSqlBuilder.append("case query_value ")
      for (taskTagRule <- taskTagRuleList) {
        selectSqlBuilder.append(" when '" + taskTagRule.queryValue + "' " +
          "then " + CommonUtils.checkQuota(tagInfo.tagValueType, taskTagRule.subTagValue) + "")
      }
      selectSqlBuilder.append(" end as tagValue from  (")
        .append(taskSql).append(") as user_value_view;")
    } else {
      selectSqlBuilder.append(" query_value as tagValue from (")
        .append(taskSql).append(") as user_value_view;")
    }
    val insertTagSql: String = insertSqlBuilder.append(selectSqlBuilder).toString()
    //执行SQL
//    println(createTableSql);
//    println(insertTagSql);
    sparkSession.sql(s" use ${wareHouseDbName}")
    sparkSession.sql(insertTagSql)

  }

}