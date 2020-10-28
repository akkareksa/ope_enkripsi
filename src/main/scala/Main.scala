
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("Enkripsi Big Data")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Enkripsi Big Data")
      .getOrCreate()
    val plaintextPath = args(0)
    val keyPath = args(1)
    val outputPath = args(2)
    val plaintext_df = spark.read.option("header","true").csv(plaintextPath)
    val cleanplaintext_df_temp = plaintext_df.selectExpr("cast(value as double) value").na.drop()
    cleanplaintext_df_temp.createOrReplaceTempView("plainTable")
    val minPlaintext_df = spark.sql("SELECT MIN(value)as minPlaintext FROM plainTable")

    val cleanplaintext_df = cleanplaintext_df_temp.crossJoin(minPlaintext_df).withColumn("id",row_number().over(Window.orderBy(lit(0)))-1)

    val key_df_temp = spark.read.option("header","true").csv(keyPath)
    key_df_temp.createOrReplaceTempView("tempTable")
    val key_df = spark.sql("SELECT CAST(bucketId as int),listWidth,CAST(minId as int),CAST(maxId as int),CAST(minValue as double),CAST(maxValue as double),CAST(width as double),CAST(quadraticCoefficient as double),CAST(ScaleFactor as double),listBucketValue,listQuadraticCoefficient,listScaleFactor,minRemainListValue,maxRemainListValue,CAST(minEncryptedValue as double) as minFlatValue,CAST(maxEncryptedValue as double) as maxFlatValue,CAST(minPredict as double),CAST(maxPredict as double),CAST(minPlaintext as double) as minP FROM tempTable")
    //    print("Tipe data key: ",key_df.)


    //sebelum ini merupakan join untuk proses gabungin
    val plainkey_df = cleanplaintext_df.join(key_df,(cleanplaintext_df("value")>=key_df("minValue"))&&(cleanplaintext_df("value")<=key_df("maxValue")))
    plainkey_df.createOrReplaceTempView("plainkeyTable")


    val bucketedPlaintext = spark.sql("SELECT * FROM plainkeyTable WHERE value>=minValue and value<=maxValue ORDER BY id ASC").dropDuplicates("id")
    bucketedPlaintext.createOrReplaceTempView("tempTable")
    bucketedPlaintext.show(10)

    spark.udf.register( "getPlaintextRemainQuadraticCoefAndScaleFactor", getPlaintextRemainQuadraticCoefAndScaleFactor _ )
    spark.udf.register( "getFlatValue", getFlatValue _ )
    spark.udf.register( "getEncryptedValue", getEncryptedValue _ )
    spark.udf.register("getEncryptBelow", getEncryptBelow _ )
    spark.udf.register("getEncryptUpper", getEncryptUpper _ )

    val map_df = spark.sql("SELECT value,minPlaintext, minFlatValue, maxFlatValue, bucketId,listWidth,minId,maxId,minValue,maxValue,width,quadraticCoefficient,ScaleFactor,listBucketValue,listQuadraticCoefficient,listScaleFactor,getPlaintextRemainQuadraticCoefAndScaleFactor(value,listWidth,listBucketValue,minP)as remainListValue, minP FROM tempTable ORDER BY bucketId ASC")
    map_df.createOrReplaceTempView("tempTable")

    val flat_df = spark.sql("SELECT bucketId,value,minP, minFlatValue, maxFlatValue,listWidth,minId,maxId,minValue,maxValue,width,quadraticCoefficient,ScaleFactor,listBucketValue,listQuadraticCoefficient,listScaleFactor, getFlatValue(listWidth,listQuadraticCoefficient,listScaleFactor,minP,value,remainListValue)as flatValue, remainListValue FROM tempTable ORDER BY value ASC")
    flat_df.createOrReplaceTempView("tempTable")

    val tempFinal = spark.sql("SELECT value, flatValue, getEncryptedValue(minValue,maxValue,value, minFlatValue, maxFlatValue, flatValue) as encryptedValue FROM tempTable ORDER BY value ASC")

    val encryptFinal = spark.sql("SELECT getEncryptedValue(minValue,maxValue,value, minFlatValue, maxFlatValue, flatValue) as value FROM tempTable ORDER BY value ASC")

    // Process below
    key_df.createOrReplaceTempView("keyTable")

    // dapetin bucket yang berisi plaintext dibawah nilai min
    val below_key = spark.sql("SELECT * FROM keyTable ORDER BY bucketId ASC LIMIT 1")
    val minEncryptVal = below_key.head().get(14).toString().toDouble

    below_key.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv("D:/compile/below/key")

    // dapetin record bucket kunci awal
    val join_below = plaintext_df.crossJoin(below_key)
    join_below.createOrReplaceTempView("belowTable")

    val plain_below_min = spark.sql("SELECT * FROM belowTable WHERE value< minP")

    // dapetin p0,p1, f0, f1
    val uniq_flat_df = tempFinal.dropDuplicates("value")
    uniq_flat_df.createOrReplaceTempView("flatTable")

    val belowRecord = spark.sql("SELECT value, encryptedValue FROM flatTable ORDER BY value ASC LIMIT 2")
    belowRecord.createOrReplaceTempView("belowTable")

    var first = spark.sql("SELECT value,encryptedValue FROM belowTable ORDER BY value ASC LIMIT 1")
    var last = spark.sql("SELECT value,encryptedValue FROM belowTable ORDER BY value DESC LIMIT 1")
    var p0b = first.head().get(0).toString().toDouble
    var f0b = first.head().get(1).toString().toDouble
    var p1b = last.head().get(0).toString().toDouble
    var f1b = last.head().get(1).toString().toDouble
    print("p0b: ",p0b," f0b: ",f0b," p1b: ",p1b," f1b: ",f1b)

    val new_below = plain_below_min.withColumn("p0b",lit(p0b)).withColumn("f0b",lit(f0b)).withColumn("p1b",lit(p1b)).withColumn("f1b",lit(f1b))
    new_below.createOrReplaceTempView("belowTable")

    val b0Encrypt = spark.sql(s"SELECT getEncryptBelow(value,p0b,f0b,p1b,f1b,${minEncryptVal}) as value FROM belowTable")

    // Process upper

    // dapetin bucket yang berisi plaintext dibawah nilai min
    val upper_key = spark.sql("SELECT * FROM keyTable ORDER BY bucketId DESC LIMIT 1")

    val maxEncryptVal = upper_key.head().get(15).toString().toDouble
    
    // dapetin record bucket kunci awal
    val join_upper = plaintext_df.crossJoin(upper_key)
    join_upper.createOrReplaceTempView("upperTable")

    val maxP = upper_key.head().get(5).toString().toDouble
    print("maxP: ",maxP)
    val plain_upper_max = spark.sql(s"SELECT * FROM upperTable WHERE value> ${maxP}")


    // dapetin p0,p1, f0, f1
    val upperRecord = spark.sql("SELECT value, encryptedValue FROM flatTable ORDER BY value DESC LIMIT 2")
    upperRecord.createOrReplaceTempView("upperTable")
    var firstU = spark.sql("SELECT value, encryptedValue FROM upperTable ORDER BY value ASC LIMIT 1")
    var lastU = spark.sql("SELECT value, encryptedValue FROM upperTable ORDER BY value DESC LIMIT 1")
    var p0u = firstU.head().get(0).toString().toDouble
    var f0u = firstU.head().get(1).toString().toDouble
    var p1u = lastU.head().get(0).toString().toDouble
    var f1u = lastU.head().get(1).toString().toDouble
    print("p0u: ",p0u," f0u: ",f0u," p1u: ",p1u," f1u: ",f1u)

    val new_upper = plain_upper_max.withColumn("p0u",lit(p0u)).withColumn("f0u",lit(f0u)).withColumn("p1u",lit(p1u)).withColumn("f1u",lit(f1u))
    new_upper.createOrReplaceTempView("upperTable")

    val unEncrypt = spark.sql(s"SELECT getEncryptUpper(value,p0u,f0u,p1u,f1u,${maxEncryptVal}) as value FROM upperTable")
    val joinub = b0Encrypt.union(unEncrypt)

    val tempFinalEncrypt = joinub.union(encryptFinal)
    tempFinalEncrypt.createOrReplaceTempView("finalTable")
    val finalEncrypt = spark.sql("SELECT * FROM finalTable ORDER BY value ASC")
    finalEncrypt.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outputPath)

    print("minEncryptedValue: ",minEncryptVal)
    print("maxEncryptedValue: ",maxEncryptVal)
    sc.stop()
  }

  def getPlaintextRemainQuadraticCoefAndScaleFactor(plaintext:Double, listWidth:String, listBucketValue: String,minPlaintext:Double): String = {
    var splitWidthString = listWidth.split(",")
    var width = splitWidthString.map(_.toDouble)
    var splitBucketValueString = listBucketValue.split(",")
    var bucket = splitBucketValueString.map(_.toDouble)
    var remain = plaintext
    for(i <- 0 to width.size-1) {
      var temp = remain-width(i)
      if(temp>=minPlaintext) {
        remain = temp
      }
    }
    var i = 0
    var status = 0
    var result = ""
    while (status==0) {
      var min=bucket(i*11+2)
      var max=bucket(i*11+3)
      var s = bucket(i*11+5)
      var z = bucket(i*11+6)
      i+=1
      if(min<=remain && max>=remain){
        status=1
        result = remain.toString()+","+s+","+z
      }
    }
    return result
  }

  def getFlatValue(listWidth:String, listQuadraticCoefficient:String, listScaleFactor:String, minPlaintext:Double, plaintext: Double, remainListValue:String): Double = {
    var splitWidthString = listWidth.split(",")
    var splitWidth = splitWidthString.map(_.toDouble)
    var splitQuadraticCoefficientString = listQuadraticCoefficient.split(",")
    var splitQuadraticCoefficient = splitQuadraticCoefficientString.map(_.toDouble)
    var splitScaleFactorString = listScaleFactor.split(",")
    var splitScaleFactor = splitScaleFactorString.map(_.toDouble)
    var remainString = remainListValue.split(",")
    var remainValue = remainString(0).toDouble
    var remainQuadraticCoefficient = remainString(1).toDouble
    var remainScaleFactor = remainString(2).toDouble
    var pMin = minPlaintext
    var p = plaintext-pMin
    var fMin = pMin
    var f = fMin
    for (i <- 0 to splitWidth.size-1) {
      var temp = p - splitWidth(i)
      if(temp>=minPlaintext) {
        p = temp
        f = f + (splitScaleFactor(i)*(splitQuadraticCoefficient(i)*splitWidth(i)*splitWidth(i)+splitWidth(i)))
      }
    }
    f = f + (remainScaleFactor*(remainQuadraticCoefficient*remainValue*remainValue+remainValue))
    return f
  }

  def getEncryptedValue(minValue:Double,maxValue:Double,value:Double, minFlatValue:Double, maxFlatValue:Double, flatValue: Double):Double = {
    var result = minFlatValue+ ((value-minValue)/(maxValue-minValue))*(maxFlatValue-minFlatValue)
    return result
  }

  def getEncryptBelow(value:Double,p0b:Double,f0b:Double,p1b:Double,f1b:Double,minEncryptedValue:Double):Double = {
    //ambil nilai plaintext untuk id 0 dan 1
    //ambil nilai flat untuk id 0 dan 1
     var z = (f1b-f0b) / (p1b-p0b)
    return minEncryptedValue + (z*value)
  }

  def getEncryptUpper(value:Double,p0u:Double,f0u:Double,p1u:Double,f1u:Double,maxEncryptedValue:Double):Double = {
    //ambil nilai plaintext untuk id 0 dan 1
    //ambil nilai flat untuk id 0 dan 1
    var z = (f1u-f0u) / (p1u-p0u)
    return maxEncryptedValue + (z*value)
  }
}

