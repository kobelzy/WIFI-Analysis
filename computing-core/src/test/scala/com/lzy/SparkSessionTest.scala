package com.lzy

import java.lang

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import sun.util.logging.resources.logging

/**
  * Created by Administrator on 2017/12/31.
  */
object SparkSessionTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//  implicit val longcoder: Encoder[Long] = org.apache.spark.sql.Encoders.scalaLong


  case class newNameCase(id: Int, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import spark.implicits._
    val data1 = spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")
      .csv("D:\\WorkSpace\\ScalaWorkSpace\\WIFI-Analysis\\computing-core\\src\\test\\resources\\test.csv")
    val data2 = spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")
      .csv("D:\\WorkSpace\\ScalaWorkSpace\\WIFI-Analysis\\computing-core\\src\\test\\resources\\test.csv")
    data1.show(false)
    import org.apache.spark.sql.functions.monotonically_increasing_id

    val data3 = data1.withColumn("idExtend", monotonically_increasing_id())
      .withColumn("idExtend", $"idExtend".cast(IntegerType))
//      .withColumn("age", $"age".cast(LongType))
    data3.show(false)
    data3.printSchema()

    val data5:Dataset[Long]=data3.select($"age".as[Long])
    data5.printSchema()
   val to= new Tokenizer()
    to.transform(data3)
    println("jianzheni ")
    //    implicit val encoder=org.apache.spark.sql.Encoders.scalaLong
//        val data4=data1.select(col("*"),data3.col("idExtend"))
//        val data4=data1.withColumn("newName", data3.col("ids"))
    val data4 = data1.withColumnRenamed("id","idExtend").withColumn("newID", data3.col("idExtend"))
    data4.show(false)
//val data6=data1.withColumn("newName2",data4.col("newName"))
//data6.show(false)
   val outputAttrs:BinaryAttribute= BinaryAttribute.defaultAttr.withName("newName")
//    val outputAttrGroup= AttributeGroup("newName",outputAttrs)

//    val outputAttrs: Array[Attribute] =
//      filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
//    outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
//  }




  }
//  @Since("2.0.0")
//  override def transform(dataset: Dataset[_], inputCol: String, outputCol: String, dropLast: Boolean): DataFrame = {
//    transformSchema(dataset.schema, logging = true)
//    val inputColSchema = dataset.schema(inputCol)
//    // If the labels array is empty use column metadata
//    val values = if (!isDefined(labels) || $(labels).isEmpty) {
//      Attribute.fromStructField(inputColSchema)
//        .asInstanceOf[NominalAttribute].values.get
//    } else {
//      $(labels)
//    }
//    val indexer = udf { index: Double =>
//      val idx = index.toInt
//      if (0 <= idx && idx < values.length) {
//        values(idx)
//      } else {
//        throw new SparkException(s"Unseen index: $index ??")
//      }
//    }
//    val outputColName =outputCol
//    val test:Column=dataset(inputCol)
//    dataset.select(col("*"),
//      indexer(dataset(inputCol).cast(DoubleType)).as(outputColName))
//  }

  def transform(dataset: Dataset[_], inputCol: String, outputCol: String, dropLast: Boolean): DataFrame = {
    // schema transformation
    val inputColName = inputCol
    val outputColName = outputCol
    val shouldDropLast = dropLast
    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema, inputCol, outputCol, dropLast)(outputColName))
    if (outputAttrGroup.size < 0) {
      // If the number of attributes is unknown, we check the values from the input column.
      val numAttrs = dataset.select(col(inputColName).cast(DoubleType)).rdd.map(_.getDouble(0))
        .aggregate(0.0)(
          (m, x) => {
            assert(x <= Int.MaxValue,
              s"OneHotEncoder only supports up to ${Int.MaxValue} indices, but got $x")
            assert(x >= 0.0 && x == x.toInt,
              s"Values from column $inputColName must be indices, but got $x.")
            math.max(m, x)
          },
          (m0, m1) => {
            math.max(m0, m1)
          }
        ).toInt + 1
      val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
      val filtered = if (shouldDropLast) outputAttrNames.dropRight(1) else outputAttrNames
      val outputAttrs: Array[Attribute] =
        filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }
    val metadata = outputAttrGroup.toMetadata()

    // data transformation
    val size = outputAttrGroup.size
    val oneValue = Array(1.0)
    val emptyValues = Array.empty[Double]
    val emptyIndices = Array.empty[Int]
    val encode: UserDefinedFunction = udf { label: Double =>
      if (label < size) {
        Vectors.sparse(size, Array(label.toInt), oneValue)
      } else {
        Vectors.sparse(size, emptyIndices, emptyValues)
      }
    }
    val test: Column = encode(col(inputColName).cast(DoubleType)).as(outputColName, metadata)
    dataset.select(col("*"), encode(col(inputColName).cast(DoubleType)).as(outputColName, metadata))
  }

  def transformSchema(schema: StructType, inputCol: String, outputCol: String, dropLast: Boolean): StructType = {
    val inputColName = inputCol
    val outputColName = outputCol

    require(schema(inputColName).dataType.isInstanceOf[NumericType],
      s"Input column must be of type NumericType but got ${schema(inputColName).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val inputAttr = Attribute.fromStructField(schema(inputColName))
    val outputAttrNames: Option[Array[String]] = inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if (dropLast) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) {
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup(outputCol, attrs)
    } else {
      new AttributeGroup(outputCol)
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }
}
