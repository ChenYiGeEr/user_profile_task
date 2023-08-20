package com.lim.userprofile.pipeline

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class PipelineUtils {

  /** 结果列列名称 */
  var labelColumnName: String = null
  /** 特征列名称数组 */
  var featureColumnNames: Array[String] = null
  /** 流水线对象 */
  private var pipeline: Pipeline = null
  /** 流水线训练后模型 */
  private var pipelineModel: PipelineModel = null
  //// 以下为优化参数 ////////////////////
  //最大分类树（用于识别连续值特征和分类特征）
  private var maxCategories = 5
  // 最大分支数
  private var maxBins = 5
  // 最大树深度
  private var maxDepth = 5
  //最小分支包含数据条数
  private var minInstancesPerNode = 1
  //最小分支信息增益
  private var minInfoGain = 0.0

  /** 设置最大分类树 */
  def setMaxCategories(maxCategories: Int): PipelineUtils = {
    this.maxCategories = maxCategories
    this
  }

  /** 设置最大分支数 */
  def setMaxBins(maxBins: Int): PipelineUtils = {
    this.maxBins = maxBins
    this
  }

  /** 设置最大树深度 */
  def setMaxDepth(maxDepth: Int): PipelineUtils = {
    this.maxDepth = maxDepth
    this
  }

  /** 设置最小分支包含数据条数 */
  def setMinInstancesPerNode(minInstancesPerNode: Int): PipelineUtils = {
    this.minInstancesPerNode = minInstancesPerNode
    this
  }

  /** 设置最小分支信息增益 */
  def setMinInfoGain(minInfoGain: Double): PipelineUtils = {
    this.minInfoGain = minInfoGain
    this
  }

  /** 设置结果列列名 */
  def setLabelColumnName(labelColumnName: String): PipelineUtils = {
    this.labelColumnName = labelColumnName
    this
  }

  /** 设置特征列名称数组 */
  def setFeatureColumnNames(featureColumnNames: Array[String]): PipelineUtils = {
    this.featureColumnNames = featureColumnNames
    this
  }

  /** 流水线初始化 */
  def init(): PipelineUtils = {
    // 检查参数是否设置
    if (labelColumnName == null) throw new RuntimeException("need set labelColumnName first! ")
    if (featureColumnNames == null) throw new RuntimeException("need set featureColumnNames first! ")
    // 初始化流水线
    pipeline = new Pipeline().setStages(Array(
      createLabelIndexer(labelColumnName),
      createFeatureAssemble(featureColumnNames),
      createFeatureIndexer(),
      createClassifier()
    ))
    this
  }

  /** 创建标签索引LabelIndexer对象 */
  def createLabelIndexer(labelColumn: String): StringIndexer = {
    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol(labelColumn).setOutputCol("label_index")
    stringIndexer
  }

  /** 创建特征集合FeaturesAssembler对象 */
  def createFeatureAssemble(featureColumn: Array[String]): VectorAssembler = {
    val vectorAssembler = new VectorAssembler()
      .setInputCols(featureColumn)
      .setOutputCol("features_assemble")
    vectorAssembler
  }

  /** 创建特征向量向量索引FeatureIndexer */
  def createFeatureIndexer(): VectorIndexer = {
    val vectorIndexer = new VectorIndexer()
      .setInputCol("features_assemble")
      .setOutputCol("features_index")
      .setHandleInvalid("skip")
      .setMaxCategories(maxCategories)
    vectorIndexer
  }

  /** 创建分类器 */
  def createClassifier(): DecisionTreeClassifier = {
    // 使用决策树分类器
    val classifier = new DecisionTreeClassifier()
      // 填入上游特征索引的名称
      .setFeaturesCol("features_index")
      // 填入上游标签索引的名称
      .setLabelCol("label_index")
      // 填入未来预测后预测结果的列名
      .setPredictionCol("prediction")
      // 可以选择gini 或者 entropy
      .setImpurity("gini")
      .setMaxBins(maxBins)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinInfoGain(minInfoGain)
      .setMaxDepth(maxDepth)
    classifier
  }

  /** 数据训练 */
  def train(dataFrame: DataFrame): Unit = {
    pipelineModel = pipeline.fit(dataFrame)
  }

  /** 训练后 获得分类树 */
  def getModelTree(): String = {
    if (pipelineModel == null) throw new RuntimeException("need training first! ")
    val classificationModel: DecisionTreeClassificationModel =
      pipelineModel.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    classificationModel.toDebugString
  }

  /** 训练后获得特征权重 */
  def getFeatureImportance(): String = {
    if (pipelineModel == null) throw new RuntimeException("need training first! ")
    val classificationModel: DecisionTreeClassificationModel
    = pipelineModel.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    classificationModel.featureImportances.toString
  }

  /** 预测 */
  def predict(dataFrame: DataFrame): DataFrame = {
    if (pipelineModel == null) throw new RuntimeException("need training first! ")
    pipelineModel.transform(dataFrame)
  }

  //  打印评估报告 // 总准确率   //各个选项的 召回率 和精确率
  def printEvaluateReport(predictedDataFrame: DataFrame): Unit = {
    val predictAndLabelRDD: RDD[(Double, Double)] = predictedDataFrame.rdd.map { row =>
      val predictValue: Double = row.getAs[Double]("prediction_col")
      val labelValue: Double = row.getAs[Double]("label_index")
      (predictValue, labelValue)

    }
    val metrics = new MulticlassMetrics(predictAndLabelRDD)
    println(" 总准确率: " + metrics.accuracy)
    metrics.labels.foreach { label =>
      println(s" 矢量值为：$label 的  精确率:  ${metrics.precision(label)}")
      println(s" 矢量值为：$label 的  召回率:  ${metrics.recall(label)}")
    }

  }

  /** 把生成的模型存储到指定的位置 */
  def saveModel(path: String): Unit = {
    if (pipelineModel == null) throw new RuntimeException("need training first! ")
    pipelineModel.write.overwrite().save(path)
  }

  /** 把已经存储到hdfs模型加载到对象中 */
  def loadModel(path: String): PipelineUtils = {
    pipelineModel = PipelineModel.load(path)
    this
  }

  //转换预测结果矢量
  def convertLabel(dataFrame: DataFrame): DataFrame = {
    if (pipelineModel == null) throw new RuntimeException("need training first! ")
    val indexerModel: StringIndexerModel =
      pipelineModel.stages(0).asInstanceOf[StringIndexerModel]
    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("prediction_origin")
      .setLabels(indexerModel.labels)
    val convertedDF: DataFrame = converter.transform(dataFrame)
    convertedDF
  }
}