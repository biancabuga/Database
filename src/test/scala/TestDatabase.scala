import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class TestDatabase extends AnyFunSpec with BeforeAndAfterAll {

  var PUNCTAJ: Double = 0

  override def afterAll(): Unit = {
    println(s"Total punctaj: $PUNCTAJ / 15")
  }

  describe("insert") {
    it("1") {
      // PART 1
      val db1 : Database = Database(List()).insert("a")
      assert(db1.tables.size == 1)
      assert(db1.tables.head.name == "a")
      assert(db1.tables.head.tableData.isEmpty)

      // PART 2
      val db2: Database = Database(List()).insert("a").insert("b")
      assert(db2.tables.size == 2)
      assert(db2.tables.head.name == "a")
      assert(db2.tables.head.tableData.isEmpty)
      assert(db2.tables(1).name == "b")
      assert(db2.tables(1).tableData.isEmpty)

      // PART 3
      val db3: Database = Database(List(Utils.people, Utils.hobbies))
        .insert("a")
        .insert("b")
        .insert("c")
      assert(db3.tables.size == 5)
      assert(db3.tables.head.name == "People")
      assert(db3.tables(1).name == "Hobbies")
      assert(db3.tables(2).name == "a")
      assert(db3.tables(3).name == "b")
      assert(db3.tables(4).name == "c")
      PUNCTAJ += 3
    }
  }

  describe("select") {
    it("1") {
      // PART 1
      val result1: Database = Utils.db1.selectTables(List("People")).get
      assert(result1.tables.size == 1)
      assert(result1.tables.head.name == "People")

      // PART 2
      val names2: List[String] = List("1", "3", "6", "8")
      val result2: Database = Utils.db2.selectTables(names2).get
      assert(result2.tables.size == 4)
      assert(result2.tables.map(_.name) == names2)

      // PART 3
      val names3: List[String] = List("2", "4", "6", "10")
      val result3: Database = Utils.db2.selectTables(names3).get
      assert(result3.tables.size == 4)
      assert(result3.tables.map(_.name) == names3)

      PUNCTAJ += 3
    }
  }

  describe("delete") {
    it("1") {
      // PART 1
      val result1: Database = Utils.db1.delete("People")
      assert(result1.tables.size == 2)

      // PART 2
      val names2: List[String] = List("1", "3", "6", "8")
      val result2: Database = names2.foldLeft(Utils.db2)((acc, name) => acc.delete(name))
      assert(result2.tables.size == 6)
      assert(result2.tables.map(_.name) == List("2", "4", "5", "7", "9", "10"))

      // PART 3
      val names3: List[String] = List("2", "4", "6", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20")
      val result3: Database = names3.foldLeft(Utils.db2)((acc, name) => acc.delete(name))
      assert(result3.tables.size == 6)
      assert(result3.tables.map(_.name) == List("1", "3", "5", "7", "8", "9"))

      PUNCTAJ += 3
    }
  }

  describe("update") {
    it("1") {
      // PART 1
      val extractedPeople: Table = Utils.db1.selectTables(List("People")).get.tables.head
      val addedPeople: Table = extractedPeople.insert(Map("id" -> "11", "name" -> "Jordan", "age" -> "33", "address" -> "331 Main St"))
      val updatedDB: Database = Utils.db1.update("People", addedPeople)
      val result: Table = updatedDB.selectTables(List("People")).get.tables.head
      val expected: String =
        """id,name,age,address
          |1,John,23,123 Main St
          |2,Jane,27,456 Elm St
          |3,Joe,30,789 Maple St
          |4,Jill,25,101 Oak St
          |5,Jack,27,112 Pine St
          |6,Jen,24,131 Cedar St
          |7,Jim,26,141 Birch St
          |8,Jesse,29,151 Spruce St
          |9,Jenny,23,161 Fir St
          |10,Jerry,28,171 Larch St
          |11,Jordan,33,331 Main St""".stripMargin

      assert(result.toString == expected)

      PUNCTAJ += 3
    }
  }

  describe("indexing") {
    it("1"){
      // PART 1
      val t1: Table = Utils.db1(0)
      val t2: Table = Utils.db1(1)
      val t3: Table = Utils.db1(2)
      assert(t1.name == "People")
      assert(t2.name == "Jobs")
      assert(t3.name == "Hobbies")

      // PART 2
      val tables = (1 to 10).map(i => Utils.db2(i - 1))
      assert(tables.map(_.name) == List("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"))

      PUNCTAJ += 3
    }

  }


}