package main.scala

import java.nio.file.Paths

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object HarvestAggregator {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
    val testDataDir = "src/main/resources"
    val fileName = "InternationalBaseline2019-Final.xlsx"
    val dir = Paths.get(".").resolve(testDataDir).toAbsolutePath
    val filePath = dir.resolve(s"$fileName").toString
    implicit val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val sheets = Seq("'Barley'!A493:B506", "'Beef'!A583:B596", "'Corn'!A637:B650", "'Cotton'!A529:B542", "'Pork'!A421:B434",
      "'Poultry'!A674:B687", "'Rice'!A727:B740", "'Sorghum'!A331:B344", "'Soybeans'!A529:B542",
      "'Soybean meal'!A565:B578", "'Soybean oil'!A565:B578", "'Wheat'!A691:B704")

    val worldHarvestSheets = Seq("'Barley'!A511:B524", "'Beef'!A601:B614", "'Corn'!A673:B686", "'Cotton'!A565:B578",
      "'Pork'!A457:B470", "'Poultry'!A710:B723", "'Rice'!A763:B776", "'Sorghum'!A349:B362", "'Soybeans'!A565:B578",
      "'Soybean meal'!A601:B614", "'Soybean oil'!A601:B614", "'Wheat'!A727:B740")

    val usaDF = getExcelDF(sheets, filePath, "usa")
    val worldDF = getExcelDF(worldHarvestSheets, filePath, "world")
    val totalHarvestDF = worldDF.join(usaDF, Seq("year"), "left")

    val sheetCategoryList = totalHarvestDF.schema.fieldNames.map(colName =>
      colName.splitAt(colName.indexOf("_") + 1)._2).filter(_ != "year").distinct

    val usaShareDf: DataFrame = sheetCategoryList.foldLeft(totalHarvestDF) {
      case (data, name) =>
        data.withColumn(s"usa_${name.split("_")(0)}_contribution%",
          when(col(s"usa_$name") === 0 || col(s"world_$name") === 0, 0)
            .otherwise((col(s"usa_$name") / col(s"world_$name")) * 100))
          .drop(s"usa_$name") }
      .na.fill(0)

    usaShareDf.show()
  }

  def extractSheetName(sheetName: String): String = {
    sheetName.replaceAll("'", "").split("!")(0).toLowerCase()
  }

  def colName(sheetName: String, countryPrefix: String, category: String): String = {
    val sheet = extractSheetName(sheetName)
    s"${countryPrefix}_${sheet}_$category"
  }

  def correctYear(year: String): String = {
    year.length match {
      case 6 => s"${year.substring(0, 4)}/${year.substring(2, 4).toInt + 1}"
      case _ => year
    }
  }

  def getExcelDF(sheets: Seq[String], filePath: String, countryPrefix: String)(implicit spark: SparkSession): DataFrame = {
    val correctYearUDF = udf((year: String) => correctYear(year))
    val sheetsDf: Seq[DataFrame] = sheets.map { sheet1 =>

      import spark.implicits._
      val df = spark.read
        .format("com.crealytics.spark.excel")
        .option("inferschema", "true")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("addColorColumns", "False")
        .option("dataAddress", sheet1)
        .load(filePath)

      val secCol = df.schema.fieldNames.filter(_ != "year")(0).toLowerCase
      df.withColumn(colName(sheet1, countryPrefix, secCol.trim), col(secCol))
        .withColumn("year", $"year".cast(StringType))
        .withColumn("year", trim(correctYearUDF($"year")))
        .drop(secCol)
    }

    val usaDF = sheetsDf.reduce((a, b) => a.join(b, Seq("year"), "left")).na.fill(0)
    usaDF
  }
}
