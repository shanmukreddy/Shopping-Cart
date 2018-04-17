# Shopping-Cart
This is web application for  for online shopping and cheking out events

 val filList :String = "col1,col2"
    val list1  :List[String]=List("a","b")
    val colList=list1.split(",")
    val matchList = Map("A" -> "c1", "B" -> "b1")
    //val colList=rb.ge
    //val rb=java.util.ResourceBundle.getBundle("common")
    //println(colList)
//val colList=rb.getString("filList").split(",")
    val df1 = df.withColumn(s"$colList+_Ind", when($"$colList" === "a1" || $"$colList" === "b1" , $"$colList").otherwise(null))
    df1.show()
    //val df3=df1.withColumn("colName"+"_new", when($"$colName" === "a1" || $"col1" === "b1" , $"col1").otherwise(null))
    val df2= df.columns.map(colName=> if(filList.contains(colName))  df1.withColumn(colName+"_new", when($"$colName" === "a1" || $"col1" === "b1" , $"col1").otherwise(null)))
    //println(df2.toString)

val dropColNames = Seq("col1")
    val featColNames = df.columns.diff(dropColNames)
    println(featColNames)
    val featCols = featColNames.map(cn => org.apache.spark.sql.functions.col(cn))
    println(featCols)
    val featsdf = df.select(featCols: _*)
    featsdf.show()




object Main {
  def main(args: Array[String]): Unit = {
    print("Hi")
    val spark = SparkSession.builder().master("local").appName("ParquetAppendMode").getOrCreate()

    import spark.sqlContext.implicits._

    //create a simple dataframe with one column

    val df_csv = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false")
      .load("src/main/scala/file.csv")
    //df_csv.show()
    val df = spark.sparkContext.parallelize(List((1, 2, 3), (4, 5, 6), (7, 8, 9))).toDF("abc", "bcd", "cde")
    df.registerTempTable("TBL")
    //df.show()

    var cols2 = Seq[Column]()
    val l = Array()

//    df.columns.foreach(row => {
//      df_csv.collect().foreach(row1 => {
//        if (row == row1.getAs[String](1)) {
//          //val df2=df.sqlContext.sql("select case when lower (" + row1.getAs[String](1) + ") in ( '" + row1.getAs[String](3) + "' ) then " + row1.getAs[Int](4) + " else " + row1.getAs[Int](2) + " end as " + row1.getAs[String](0) + " from TBL ")}
//          val cc:Column =  df.col(row1.getAs[String](1)).when(df.col(row1.getAs[String](1)).isin(row1.getAs[String](3)), lit(row1.getAs[String](1)))
//          //.otherwise(lit(row1.getAs[String](2)))
//          //val cc:Column =  df.select(when(df.col(row1.getAs[String](1)).equals(row1.getAs[String](3)), lit(row1.getAs[String](1)))
//          //.otherwise(lit(row1.getAs[String](2))))
//
//          //people.select(when(people("gender") === "male", 0).when(people("gender") === "female", 1).otherwise(2))
//
//          //val cc:Column =  df.select(when(row1.getAs[String](3) === row1.getAs[String](3),row1.getAs[Int](1)).otherwise(lit(row1.getAs[String](2))))
//          //df.select(row1.getAs[String](3))
//          //cols2 = cols2 :+ cc
//          //println("col size "+cols2.size)
//
//        }
//          )
//        else{
//
//        }
//      })
//
//    })
val colset1 = new java.util.HashSet[String]
    val colset2 = new mutable.HashSet[String]
//    df.columns.foreach(row => {
//      df_csv.collect().foreach(row1 =>
//        df.withColumn(
//          row,
//          when(col(row) === lit(row1.getAs[String](3)), col(row1.getAs[String](1))).otherwise(col(row1.getAs[String](2)))
//        )
//      })
//
//    })


        val df2=df.columns.foreach(colu => {
          df_csv.collect().foreach(row => {
            if (colu == row.getAs[String](1)) {
              colset2.add(colu)
              df.withColumn(row.getAs[String](0),when(col(colu) === row.getAs[String](3), col(row.getAs[String](4)).toString()).otherwise(col(row.getAs[String](2)).toString()))
              //cols2 = cols2 :+ cc
              //println("col size "+cols2.size)
              //caseQyery.append("case when lower (" + row.getAs[String](1) + ") in ( '" + row.getAs[String](3) + "' ) then " + row.getAs[Int](4) + " else " + row.getAs[Int](2) + " end as " + row.getAs[String](0) + ",")
            }
          })
        })
        df.columns.foreach(colVal => {
          if (!colset2.contains(colVal))
           df.select(colVal).as(colVal)
        })


//println(cols2.size)
//df.select(cols2:_*).show()


  }
//  def check() {
//    print("Hi")
//    val spark = SparkSession.builder().master("local").appName("ParquetAppendMode").getOrCreate()
//
//
//
//    //create a simple dataframe with one column
//
//    val df_csv = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false")
//      .load("src/main/scala/file.csv")
//    df_csv.printSchema()
//    val df = spark.sparkContext.parallelize(List((1, 2, 3), (4, 5, 6), (7, 8, 9))).toDF("abc", "bcd", "cde")
//    df.registerTempTable("TBL")
//    //df.columns.foreach(println)
//    //def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]
//    print("Final result")
//    val caseQyery = new StringBuilder
//    val caseQyery1 = new StringBuilder
//    val caseQyery2 = new StringBuilder
//    val colset1 = new java.util.HashSet[String]
//    val colset2 = new mutable.HashSet[String]
//    df.columns.foreach(col => {
//      df_csv.collect().foreach(row => {
//        if (col == row.getAs[String](1)) {
//          colset2.add(col)
//          caseQyery.append("case when lower (" + row.getAs[String](1) + ") in ( '" + row.getAs[String](3) + "' ) then " + row.getAs[Int](4) + " else " + row.getAs[Int](2) + " end as " + row.getAs[String](0) + ",")
//        }
//      })
//    })
//    df.columns.foreach(x => {
//      if (!colset2.contains(x))
//        caseQyery.append(x + " as " + x + ",")
//    })
//    val ndf = df.sqlContext.sql("select " + caseQyery.toString().stripSuffix(",") + " from TBL")
//    ndf.show()
//  }
}












++++++++++++++++++++++++++++++++++++++++++++++++++++


var cols2=Seq[sql.Column]() //Cols: "::" size=1
    val colset2 = new mutable.HashSet[String]
    val l = Array()

    df.columns.foreach(row => {
      df_csv.collect().foreach(row1 => {
        if (row == row1.getAs[String](1)) {
          colset2.add(row)
          val cc = when(df.col(row) === row1.getAs[String](3), row1.getAs[String](4)).otherwise(lit(row1.getAs[String](2))).as(row1.getAs[String](0))
          cols2 = cols2 :+ cc
        }

      })
    })

    df.columns.foreach(colVal => {
      if (!colset2.contains(colVal)){
        val cc = when(df.col(colVal).isNotNull,df.col(colVal)).otherwise(null).as(colVal)
        cols2 = cols2 :+ cc}
    })


    df.select(cols2: _*).show()
