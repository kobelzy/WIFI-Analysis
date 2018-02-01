package com.ml.kaggle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/1/27.
  */
object Hource_price {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hourse_price")
      .master("local[*]")
      .getOrCreate()
    /**
      * 数据准备
      */
    val test = spark.read
      .option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\test.csv")

    val train = spark.read
      .option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\train.csv")
    //    train.show(10,truncate = false)
    //    test.show(10,truncate = false)
    import spark.implicits._
    val all_df: DataFrame = train
      //      .drop("SalePrice")
      //      .union(test)
      .withColumn("MSSubClass", $"MSSubClass".cast(StringType))
    //    all_df.printSchema()
    /*
//    修改schema，但是失败了。
     */
    /*al MSSubClass_new:DataFrame=all_df.select($"MSSubClass".cast(StringType))
//    MSSubClass_new.printSchema()
//    MSSubClass_new.show(10,false)
    val all_df2=all_df
      .withColumn("MSSubClass",MSSubClass_new.col("MSSubClass"))
    all_df2.printSchema()
    all_df2.show(10,false)*/
    //    all_df.show(10, truncate = false)
    //label,为了将其平滑，使用了log
    val y_train = train.select("SalePrice").map(row => Math.log1p(row.getAs[Int](0)))
    //        y_train.show(10, false)

    /**
      * 特征工程
      */
    val pipeLine = new Pipeline()
    val pipelineStages = ArrayBuffer[PipelineStage]()

    val col_stringType_List: List[String] = all_df.schema.toList.filter(_.dataType == StringType).map(_.name)
    // 或者   val col_stringType_List2:immutable.IndexedSeq[Any]="MSSubClass"+:all_df.dtypes.map(_._2).filter(_ == "StringType")
    println("String类型字段数：" + col_stringType_List.size)
    val col_numericerType_haveId_List: List[StructField] = all_df.schema.toList.filter(_.dataType == IntegerType)

    //      .toBuffer-="Id"-="MSSubClass"
    val col_numericerType_List = col_numericerType_haveId_List.filter(_.name != "SalePrice").tail
    println(col_numericerType_List)

    println("数值类型字段数：" + col_numericerType_List.size)
    //    println(col_numericerType_List.map(_.name))

    //1、使用均值替代空值
    //生成每一列的平均值
    val column2meanMap: Map[String, Int] = getMeans(col_numericerType_List, all_df)
    println(column2meanMap)
    val all_df_notNull = all_df.na.fill(column2meanMap)
    //    all_df_notNull.show(20,false)
    //2、使用独热编码来替代数据
    //MSSubClass是一个category列，但是被定义为了Integer,需要这里只是将其作为需要独热编码的字段。

    takeStringIndexer(col_stringType_List, pipelineStages)
    takeOneHot(col_stringType_List, pipelineStages)

    //3、标准化numerical类型数据
    //将所有数值类的数据转为同一个向量。
    val vectorAssembler = new VectorAssembler()
      .setInputCols(col_numericerType_List.map(_.name).toArray)
      .setOutputCol("numericals")
    pipelineStages += vectorAssembler
    //将numericals的数据按照标准差缩放进行标准化
    val standardScaler = new StandardScaler()
      .setInputCol("numericals").setOutputCol("standarScaler_numericals")
//    pipelineStages += standardScaler
//或者按照正则化缩放
    val normalizer=new Normalizer()
      .setInputCol("numericals").setOutputCol("standarScaler_numericals")
    pipelineStages += normalizer

    //4、全部特征归一化
    val vectorAssemblerFeatures = new VectorAssembler()
      .setOutputCol("features")
    //将被Onehot的结果与numericals放到一起。
    val outputArray = col_stringType_List.map(_ + "_onehoted").toBuffer += "numericals"
    vectorAssemblerFeatures.setInputCols(outputArray.toArray)
    pipelineStages += vectorAssemblerFeatures

    /**
      * 构建模型
      */
    //1、线性回归器
    val lr = new LinearRegression()
      .setMaxIter(50)
      .setLabelCol("SalePrice")
//    pipelineStages += lr
    //随机僧林
    val randomForest = new RandomForestRegressor()
    //决策树
    val decisionTree=new DecisionTreeRegressor()
      .setLabelCol("SalePrice")
    pipelineStages+=decisionTree
    //2、构建参数矩阵
    val paramGrid = new ParamGridBuilder()
//      .addGrid(lr.regParam, Array(0.1, 0.01))
        .addGrid(decisionTree.maxBins,Array(128))
        .addGrid(decisionTree.minInstancesPerNode,Array(1,5,10))
      .build()
    //3、构建评估器
    val regressionEvaluator = new RegressionEvaluator()
      .setLabelCol("SalePrice")
    //4、构建检验器
    val trainEvaluator = new TrainValidationSplit()
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(regressionEvaluator)
      .setTrainRatio(0.8)

    //构建工作流
    pipeLine.setStages(pipelineStages.toArray)
    trainEvaluator.setEstimator(pipeLine)
    val model = trainEvaluator.fit(all_df_notNull)
    model.validationMetrics.zip(model.getEstimatorParamMaps).foreach(println(_))
    println(model.getEvaluator.isLargerBetter)
    //    val testRestul = model.transform(all_df_notNull.limit(20))
    //    testRestul.select("features").show(false)
    //    println(testRestul.columns.length)
  }


  /**
    * 设置String转index，将df中所有的String类型进行转换
    *
    * @param cols
    * @param pipelineStages
    * @return
    */
  def takeStringIndexer(cols: List[String], pipelineStages: ArrayBuffer[PipelineStage]) = {
    cols.foreach(col => {
      val indexerName = "str2Index_" + col
      val outputCol = col + "_indexed"
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(outputCol)
        .setHandleInvalid("keep")
      pipelineStages += indexer
    })

  }

  def takeOneHot(cols: List[String], pipelineStages: ArrayBuffer[PipelineStage]) = {
    cols.foreach(col => {
      val inputCol = col + "_indexed"
      val indexerName = "str2Index_" + col
      val outputCol = col + "_onehoted"
      val indexer = new OneHotEncoder().setInputCol(inputCol).setOutputCol(outputCol).setDropLast(false)
      pipelineStages += indexer
    })
  }

  /**
    * 返回指定DF中指定列的平均值，
    *
    * @param col_numericerType_List
    * @param all_df
    * @return Map[name, means]
    */
  def getMeans(col_numericerType_List: List[StructField], all_df: DataFrame): Map[String, Int] = {
    implicit val matchError = org.apache.spark.sql.Encoders.scalaInt
    //想法：按照每一个给定的数据来求出其平均数
    val map = scala.collection.mutable.Map[String, Int]()
    col_numericerType_List.foreach(structField => {
      val name = structField.name
      val colDS = all_df.select(name).as[Int]
      //      val colDS: Dataset[Double] = structField.dataType.typeName match {
      //        case "integer" => all_df.select(name).map(row => row.getAs[Int](0).toDouble)
      //        case "double" => all_df.select(name).map(_.getAs[Double](0))
      //        case "long" => all_df.select(name).map(_.getAs[Long](0).toDouble)
      //      }
      val colCount = colDS.filter(colDS(name).isNotNull).count()
      val mean = colDS.reduce(_ + _) / colCount
      map += (name -> mean.toInt)
    })
    map.toMap

  }
}
