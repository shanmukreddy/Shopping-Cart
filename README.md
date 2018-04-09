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

