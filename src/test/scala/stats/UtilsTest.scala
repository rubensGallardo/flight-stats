package stats
import org.scalatest.funsuite.AnyFunSuite
import stats.test.SparkSessionTestWrapper


class UtilsTest extends AnyFunSuite
  with SparkSessionTestWrapper {

  test ("1. Test open CSV file") {

    val utils = new Utils()
    val passengerData = utils.readCsvFile("src/test/resources/emptyPassengers.csv", spark)
    assert(passengerData.isEmpty)

  }

  test ("2. Test open small CSV file") {
    val utils = new Utils()
    val passengerData = utils.readCsvFile("src/test/resources/smallListPassengers.csv", spark)
    val result = passengerData.collect()

    assert(result.length == 4)
  }
}
