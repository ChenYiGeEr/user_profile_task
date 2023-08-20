package com.lim.userprofile.train

import com.lim.userprofile.pipeline.PipelineUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/** 学生性别训练 */
object StudentGenderTrain {

  def main(args: Array[String]): Unit = {
    println("开始查询数据.....")
    // sparkSession
    val sparkConf: SparkConf = new SparkConf().setAppName("student_gender_test") setMaster ("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sql =
      """
        | select uid,
        |       case hair
        |           when '长发' then 101
        |           when '短发' then 102
        |           when '板寸' then 103 end as hair,
        |       height,
        |       case skirt
        |           when '是' then 11
        |           when '否' then 12 end as skirt,
        |       case age
        |           when '80后' then 80
        |           when '90后' then 90
        |           when '00后' then 100 end as age,
        |       gender
        |from default.student
        """.stripMargin
    val dataFrame: DataFrame = sparkSession.sql(sql)
    dataFrame.show(120, truncate = false);
    println("切分数据.....")
    // 将数据分为两部分 70%训练数据 30%测试数据
    val Array(trainDF,testDF): Array[Dataset[Row]] = dataFrame.randomSplit(Array(0.7,0.3))
    println("开始初始化流水线.....")
    val myPipeline: PipelineUtils = new PipelineUtils()
      .setLabelColumnName("gender")
      .setFeatureColumnNames(Array("hair", "height", "skirt", "age"))
      .setMaxCategories(5)
      .setMaxDepth(6)
      .setMaxBins(3)
      .init()

    println("开始初始化训练......")
    myPipeline.train(trainDF)
    println("获得模型......")
    println(myPipeline.getModelTree())
    println("获得特征权重......")
    println(myPipeline.getFeatureImportance())

    println("开始预测测试数据......")
    val predictDF: DataFrame = myPipeline.predict(testDF)
    predictDF.show(120, truncate = false)
  }

}
