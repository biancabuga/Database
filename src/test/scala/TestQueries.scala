import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class TestQueries extends AnyFunSpec with BeforeAndAfterAll {

  var PUNCTAJ: Double = 0

  override def afterAll(): Unit = {
    println(s"Total punctaj: $PUNCTAJ / 10")
  }


  describe("query1") {
    it("1") {

      val result = Queries.query_1(Utils.db3, 29, List("Bucharest", "New York")).get
      val expectedTable = Table(
        "CustomersRef",
        "id,age,city\n" +
        "10,32,New York\n" +
        "16,31,New York\n" +
        "19,35,Bucharest")
      assert(result.tableData == expectedTable.tableData)

      PUNCTAJ += 3
    }
  }

  describe("query2") {
  it("1") {
    val result = Queries.query_2(Utils.db3, "2016-01-01", 4).get
    val expectedTable = Table(
      "OrdersRef",
      "order_id,cost\n" +
        "10,99\n" +
        "8,84\n" +
        "2,78\n" +
        "15,77\n" +
        "11,72\n" +
        "5,58\n" +
        "20,53\n" +
        "13,41\n" +
        "7,39\n" +
        "3,36\n" +
        "16,31\n" +
        "18,20"
    )
    assert(result.tableData == expectedTable.tableData)
    PUNCTAJ += 3

  }
  }
  describe("query3") {
    it("1") {
      val result = Queries.query_3(Utils.db3, 60).get
      val expectedTable = Table(
        "Ref",
        "order_id,employee_id,cost\n" +
          "6,1,66\n" +
          "11,1,72\n" +
          "2,2,78\n" +
          "12,2,63\n" +
          "17,2,88\n" +
          "8,3,84\n" +
          "4,4,92\n" +
          "19,4,90\n" +
          "10,5,99\n" +
          "15,5,77"
      )
      assert(result.tableData == expectedTable.tableData)
      PUNCTAJ += 4

    }
  }
}