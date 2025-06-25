import scala.util.Try
object Queries {
  def query_1(db: Database, ageLimit: Int, cities: List[String]): Option[Table] = {
    db.tables.find(_.name == "Customers").map(
      _.filter(Field("age", valueStr => Try(valueStr.toInt > ageLimit).getOrElse(false)) &&
            Field("city", valueStr => cities.contains(valueStr))
        ).sort("ID")
    )
  }

  def query_2(db: Database, date: String, employeeID: Int): Option[Table] = {
    db.tables.find(_.name == "Orders").map { ordersTable =>
      val dateCondition = new FilterCond {
        override def eval(r: Row): Option[Boolean] =
          r.get("date").map(valueStr => valueStr > date)
      }
      val employeeCondition = new FilterCond {
        override def eval(r: Row): Option[Boolean] =
          r.get("employee_id").flatMap(valueStr => Try(valueStr.toInt != employeeID).toOption)
      }

      val combinedFilter = dateCondition && employeeCondition
      val filteredData = ordersTable.tableData.filter(row => combinedFilter.eval(row).contains(true))
      val selectedData = filteredData.map { row =>
        List("order_id", "cost").map(c => c -> row.getOrElse(c, "")).toMap
      }
      val sortedData = selectedData.sortBy(row => Try(row.getOrElse("cost", "0").toInt).getOrElse(Int.MinValue))(Ordering[Int].reverse)
      Table(ordersTable.tableName, sortedData)
    }
  }

  def query_3(db: Database, minCost: Int): Option[Table] = {
    db.tables.find(_.name == "Orders").map(
      _.filter(
          Field("cost", valueStr => Try(valueStr.toInt > minCost).getOrElse(false))
        )
        .select(List("order_id", "employee_id", "cost"))
        .sort("employee_id")
    )
  }
}
