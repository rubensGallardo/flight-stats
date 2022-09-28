package stats.test

import stats.test.SparkSessionTestWrapper
import org.scalatest.FunSpec

class StatsTest
  extends FunSpec
  with SparkSessionTestWrapper {

  it("test flights by month"){
    val columns = Seq("passengerId","flightId","from","to","date")
    val data = Seq (("48","0","cg","ir","2017-01-01"),("967","103","tk","co","2017-02-04"))
    val flightsDf = spark.createDataFrame(data).toDF(columns:_*)
    
    
  }


}
