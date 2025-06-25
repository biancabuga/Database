import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class TestTable extends AnyFunSpec with BeforeAndAfterAll {

  var PUNCTAJ: Double = 0

  override def afterAll(): Unit = {
    println(s"Total punctaj: $PUNCTAJ / 45")
  }

  describe("apply") {
    it("1") {
      assert(Table("People", Utils.peopleStr).tableData == Utils.people.tableData)
      assert(Table("Jobs", Utils.jobsStr).tableData == Utils.jobs.tableData)
      assert(Table("Hobbies", Utils.hobbiesStr).tableData == Utils.hobbies.tableData)
      PUNCTAJ += 4
    }
  }

  describe("toString") {
    it("1") {
      assert(Utils.people.toString == Utils.peopleStr)
      assert(Utils.jobs.toString == Utils.jobsStr)
      assert(Utils.hobbies.toString == Utils.hobbiesStr)

      PUNCTAJ += 3
    }
  }

  describe("fromCSV") {
    it("1") {
      val peopleToString = Utils.people.toString
      val tableFromCsv = Table.fromCSV(peopleToString)
      val peopleToString2 = tableFromCsv.toString
      assert(peopleToString2 == Utils.peopleStr)

      val citiesFromCsv = Table.fromCSV(Utils.citiesStr)
      assert(citiesFromCsv.toString == Utils.cities.toString)
      PUNCTAJ += 3
    }
  }

  describe("filter") {
    it("1") {
      // PART 1
      val filter1: FilterCond = Field("id", _ == "1") || Field("age", a => a > "23" && a < "29")
      val result1: Table = Utils.people.filter(filter1)
      val expectedResult1: Table = Table("PeopleRef",
        "id,name,age,address\n" +
          "1,John,23,123 Main St\n" +
          "2,Jane,27,456 Elm St\n" +
          "4,Jill,25,101 Oak St\n" +
          "5,Jack,27,112 Pine St\n" +
          "6,Jen,24,131 Cedar St\n" +
          "7,Jim,26,141 Birch St\n" +
          "10,Jerry,28,171 Larch St")
      assert(result1.tableData == expectedResult1.tableData)

      // PART 2
      val filter2: FilterCond = Field("salary", a => a.toInt > 100000)
      val result2: Table = Utils.jobs.filter(filter2)
      val expectedResult2: Table = Table("JobsRef",
        "id,title,salary,person_name\n" +
          "2,Manager,200000,Jane\n" +
          "3,CEO,300000,Joe\n" +
          "4,Banker,150000,Mona\n" +
          "5,Doctor,200000,Jack\n" +
          "8,Engineer,120000,Mimi\n" +
          "9,Programmer,250000,Jenny\n" +
          "10,Teacher,400000,Jerry")
      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val filter3: FilterCond = Field("hobby", _ != "Dancing")
      val result3: Table = Utils.hobbies.filter(filter3)
      val expectedResult3: Table = Table("HobbiesRef",
        "id,name,hobby\n" +
          "1,John,Basketball\n" +
          "2,Jane,Skiing\n" +
          "3,Joe,Football\n" +
          "4,Mona,Painting\n" +
          "5,Jack,Clay modeling\n" +
          "6,Jen,Film making\n" +
          "7,Maria,Skin care\n" +
          "8,Mimi,Music\n" +
          "9,Jenny,Makeup")
      assert(result3.tableData == expectedResult3.tableData)

      PUNCTAJ += 4
    }
  }

  describe("delete") {
    it("1") {
      // PART 1
      val row1: Map[String, String] = Map("id" -> "1", "name" -> "John", "age" -> "23", "address" -> "123 Main St")
      val result1: Table = Utils.people.delete(row1)
      val expectedResult1: Table = Table("PeopleRef",
        "id,name,age,address\n" +
          "2,Jane,27,456 Elm St\n" +
          "3,Joe,30,789 Maple St\n" +
          "4,Jill,25,101 Oak St\n" +
          "5,Jack,27,112 Pine St\n" +
          "6,Jen,24,131 Cedar St\n" +
          "7,Jim,26,141 Birch St\n" +
          "8,Jesse,29,151 Spruce St\n" +
          "9,Jenny,23,161 Fir St\n" +
          "10,Jerry,28,171 Larch St")
      assert(result1.tableData == expectedResult1.tableData)

      // PART 2
      val row2: Map[String, String] = Map("id" -> "3", "title" -> "CEO", "salary" -> "300000", "person_name" -> "Joe")
      val result2: Table = Utils.jobs.delete(row2)
      val expectedResult2: Table = Table("JobsRef",
        "id,title,salary,person_name\n" +
          "1,Engineer,100000,John\n" +
          "2,Manager,200000,Jane\n" +
          "4,Banker,150000,Mona\n" +
          "5,Doctor,200000,Jack\n" +
          "6,Nurse,100000,Jen\n" +
          "7,Teacher,80000,Maria\n" +
          "8,Engineer,120000,Mimi\n" +
          "9,Programmer,250000,Jenny\n" +
          "10,Teacher,400000,Jerry")
      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val row3: Map[String, String] = Map("id" -> "10", "name" -> "Jerry", "hobby" -> "Dancing_not_existing")
      val result3: Table = Utils.hobbies.delete(row3)
      assert(result3.tableData == Utils.hobbies.tableData)

      PUNCTAJ += 3
    }
  }

  describe("insert") {
    it("1") {
      // PART 1
      val row1: Map[String, String] = Map("id" -> "11", "name" -> "Jill", "age" -> "25", "address" -> "101 Oak St")
      val result1: Table = Utils.people.insert(row1)
      val expectedResult1: Table = Table("PeopleRef",
        "id,name,age,address\n" +
          "1,John,23,123 Main St\n" +
          "2,Jane,27,456 Elm St\n" +
          "3,Joe,30,789 Maple St\n" +
          "4,Jill,25,101 Oak St\n" +
          "5,Jack,27,112 Pine St\n" +
          "6,Jen,24,131 Cedar St\n" +
          "7,Jim,26,141 Birch St\n" +
          "8,Jesse,29,151 Spruce St\n" +
          "9,Jenny,23,161 Fir St\n" +
          "10,Jerry,28,171 Larch St\n" +
          "11,Jill,25,101 Oak St")
      assert(result1.tableData == expectedResult1.tableData)

      // PART 2
      val row2: Map[String, String] = Map("id" -> "11", "title" -> "Teacher", "salary" -> "400000", "person_name" -> "Jerry")
      val result2: Table = Utils.jobs.insert(row2)
      val expectedResult2: Table = Table("JobsRef",
        "id,title,salary,person_name\n" +
          "1,Engineer,100000,John\n" +
          "2,Manager,200000,Jane\n" +
          "3,CEO,300000,Joe\n" +
          "4,Banker,150000,Mona\n" +
          "5,Doctor,200000,Jack\n" +
          "6,Nurse,100000,Jen\n" +
          "7,Teacher,80000,Maria\n" +
          "8,Engineer,120000,Mimi\n" +
          "9,Programmer,250000,Jenny\n" +
          "10,Teacher,400000,Jerry\n" +
          "11,Teacher,400000,Jerry")
      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val row3: Map[String, String] = Map("id" -> "3", "name" -> "Joe", "hobby" -> "Football")
      val result3: Table = Utils.hobbies.insert(row3)
      assert(result3.tableData == Utils.hobbies.tableData)

      PUNCTAJ += 3
    }
  }

  describe("sort") {
    it("1") {
      // PART 1
      val result1: Table = Utils.people.sort("name")
      val expectedResult1: Table = Table("PeopleRef",
        """id,name,age,address
          |5,Jack,27,112 Pine St
          |2,Jane,27,456 Elm St
          |6,Jen,24,131 Cedar St
          |9,Jenny,23,161 Fir St
          |10,Jerry,28,171 Larch St
          |8,Jesse,29,151 Spruce St
          |4,Jill,25,101 Oak St
          |7,Jim,26,141 Birch St
          |3,Joe,30,789 Maple St
          |1,John,23,123 Main St""".stripMargin)
      assert(result1.tableData == expectedResult1.tableData)

      // PART 2
      val result2: Table = Utils.jobs.sort("title")
      val expectedResult2: Table = Table("JobsRef",
        """id,title,salary,person_name
          |4,Banker,150000,Mona
          |3,CEO,300000,Joe
          |5,Doctor,200000,Jack
          |1,Engineer,100000,John
          |8,Engineer,120000,Mimi
          |2,Manager,200000,Jane
          |6,Nurse,100000,Jen
          |9,Programmer,250000,Jenny
          |7,Teacher,80000,Maria
          |10,Teacher,400000,Jerry""".stripMargin)

      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val result3: Table = Utils.hobbies.sort("hobby")
      val expectedResult3: Table = Table("HobbiesRef",
        "id,name,hobby\n" +
          "1,John,Basketball\n" +
          "5,Jack,Clay modeling\n" +
          "10,Jerry,Dancing\n" +
          "6,Jen,Film making\n" +
          "3,Joe,Football\n" +
          "9,Jenny,Makeup\n" +
          "8,Mimi,Music\n" +
          "4,Mona,Painting\n" +
          "2,Jane,Skiing\n" +
          "7,Maria,Skin care\n")
      assert(result3.tableData == expectedResult3.tableData)

      // PART4
      val result4: Table = Utils.people.sort("address", false)
      val expectedResult4: Table = Table("PeopleRef",
        """id,name,age,address
          |3,Joe,30,789 Maple St
          |2,Jane,27,456 Elm St
          |10,Jerry,28,171 Larch St
          |9,Jenny,23,161 Fir St
          |8,Jesse,29,151 Spruce St
          |7,Jim,26,141 Birch St
          |6,Jen,24,131 Cedar St
          |1,John,23,123 Main St
          |5,Jack,27,112 Pine St
          |4,Jill,25,101 Oak St""".stripMargin)
      assert(result4.tableData == expectedResult4.tableData)
      PUNCTAJ += 3
    }
  }

  describe("select") {
    it("1") {
      // PART 1
      val result1: Table = Utils.people.select(List("name", "age"))
      val expectedResult1: Table = Table("PeopleRef",
        "name,age\n" +
          "John,23\n" +
          "Jane,27\n" +
          "Joe,30\n" +
          "Jill,25\n" +
          "Jack,27\n" +
          "Jen,24\n" +
          "Jim,26\n" +
          "Jesse,29\n" +
          "Jenny,23\n" +
          "Jerry,28")
      assert(result1.tableData == expectedResult1.tableData)

      // PART 2
      val result2: Table = Utils.jobs.select(List("title", "salary"))
      val expectedResult2: Table = Table("JobsRef",
        "title,salary\n" +
          "Engineer,100000\n" +
          "Manager,200000\n" +
          "CEO,300000\n" +
          "Banker,150000\n" +
          "Doctor,200000\n" +
          "Nurse,100000\n" +
          "Teacher,80000\n" +
          "Engineer,120000\n" +
          "Programmer,250000\n" +
          "Teacher,400000")
      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val result3: Table = Utils.hobbies.select(List("name", "hobby"))
      val expectedResult3: Table = Table("HobbiesRef",
        "name,hobby\n" +
          "John,Basketball\n" +
          "Jane,Skiing\n" +
          "Joe,Football\n" +
          "Mona,Painting\n" +
          "Jack,Clay modeling\n" +
          "Jen,Film making\n" +
          "Maria,Skin care\n" +
          "Mimi,Music\n" +
          "Jenny,Makeup\n" +
          "Jerry,Dancing")
      assert(result3.tableData == expectedResult3.tableData)

      PUNCTAJ += 3
    }
  }
  describe("cartesianProduct") {
    it("1") {
      // PART 1
      val t1: Table = Utils.countriesMini
      val t2: Table = Utils.citiesMini
      val t3: Table = t1.cartesianProduct(t2)
      val expectedResult =
        """|Cities.id_city,Countries.country,Countries.id,Cities.country_id,Cities.city
          |1,Romania,1,1,Bucharest
          |2,Romania,1,2,Paris
          |3,Romania,1,3,Berlin
          |1,France,2,1,Bucharest
          |2,France,2,2,Paris
          |3,France,2,3,Berlin
          |1,Germany,3,1,Bucharest
          |2,Germany,3,2,Paris
          |3,Germany,3,3,Berlin""".stripMargin
      assert(t3.toString == expectedResult)
      PUNCTAJ += 7.5
    }
  }
  describe("join") {
    it("1") {
      val people: Table = Table(
        "People",
        List(
          Map("name" -> "John", "age" -> "23", "address" -> "123 Main St"),
          Map("name" -> "Jane", "age" -> "27", "address" -> "456 Elm St"),
          Map("name" -> "Joe", "age" -> "30", "address" -> "789 Maple St"),
          Map("name" -> "Jill", "age" -> "25", "address" -> "101 Oak St"),
          Map("name" -> "Jack", "age" -> "27", "address" -> "112 Pine St"),
          Map("name" -> "Jen", "age" -> "24", "address" -> "131 Cedar St"),
          Map("name" -> "Jim", "age" -> "26", "address" -> "141 Birch St"),
          Map("name" -> "Jesse", "age" -> "29", "address" -> "151 Spruce St"),
          Map("name" -> "Jenny", "age" -> "23", "address" -> "161 Fir St"),
          Map("name" -> "Jerry", "age" -> "28", "address" -> "171 Larch St")
        )
      )

      val jobs: Table = Table(
        "Jobs",
        List(
          Map("title" -> "Engineer", "salary" -> "100000", "person_name" -> "John", "address" -> "654 Oak St"),
          Map("title" -> "Manager", "salary" -> "200000", "person_name" -> "Jane", "address" -> "123 Main St"),
          Map("title" -> "CEO", "salary" -> "300000", "person_name" -> "Joe", "address" -> "789 Maple St"),
          Map("title" -> "Banker", "salary" -> "150000", "person_name" -> "Mona", "address" -> "789 Maple St"),
          Map("title" -> "Doctor", "salary" -> "200000", "person_name" -> "Jack", "address" -> "112 Pine St"),
          Map("title" -> "Nurse", "salary" -> "100000", "person_name" -> "Jen", "address" -> "132 Cedar St"),
          Map("title" -> "Teacher", "salary" -> "80000", "person_name" -> "Jill", "address" -> "888 Elm St"),
          Map("title" -> "Engineer", "salary" -> "120000", "person_name" -> "Mimi", "address" -> "141 Birch St"),
          Map("title" -> "Programmer", "salary" -> "250000", "person_name" -> "Jenny", "address" -> "161 Fir St"),
          Map("title" -> "Teacher", "salary" -> "400000", "person_name" -> "Jerry", "address" -> "171 Larch St")
        )
      )

      val db: Database = Database(List(people, jobs))
      val result: Table = people.join(jobs)("name", "person_name")

      val expected: String =
        """name,age,salary,address,title
          |John,23,100000,123 Main St;654 Oak St,Engineer
          |Jane,27,200000,456 Elm St;123 Main St,Manager
          |Joe,30,300000,789 Maple St,CEO
          |Jill,25,80000,101 Oak St;888 Elm St,Teacher
          |Jack,27,200000,112 Pine St,Doctor
          |Jen,24,100000,131 Cedar St;132 Cedar St,Nurse
          |Jenny,23,250000,161 Fir St,Programmer
          |Jerry,28,400000,171 Larch St,Teacher
          |Jim,26,,141 Birch St,
          |Jesse,29,,151 Spruce St,
          |Mona,,150000,789 Maple St,Banker
          |Mimi,,120000,141 Birch St,Engineer""".stripMargin

      assert(result.toString == expected)
      PUNCTAJ += 7.5
    }
  }
  describe("update") {
    it("1") {
      // PART 1
      val update1: Map[String, String] = Map("name" -> "John", "age" -> "74", "address" -> "123 Main St")
      val result1: Table = Utils.people.update(Field("name", _ == "John"), update1)
      val expectedResult: Table = Table("PeopleRef",
        "id,name,age,address\n" +
          "1,John,74,123 Main St\n" +
          "2,Jane,27,456 Elm St\n" +
          "3,Joe,30,789 Maple St\n" +
          "4,Jill,25,101 Oak St\n" +
          "5,Jack,27,112 Pine St\n" +
          "6,Jen,24,131 Cedar St\n" +
          "7,Jim,26,141 Birch St\n" +
          "8,Jesse,29,151 Spruce St\n" +
          "9,Jenny,23,161 Fir St\n" +
          "10,Jerry,28,171 Larch St")
      assert(result1.tableData == expectedResult.tableData)

      // PART 2
      val update2: Map[String, String] = Map("title" -> "Drug Dealer", "salary" -> "8000000000")
      val result2: Table = Utils.jobs.update(Field("title", _ == "Engineer"), update2)
      val expectedResult2: Table = Table("JobsRef",
        "id,title,salary,person_name\n" +
          "1,Drug Dealer,8000000000,John\n" +
          "2,Manager,200000,Jane\n" +
          "3,CEO,300000,Joe\n" +
          "4,Banker,150000,Mona\n" +
          "5,Doctor,200000,Jack\n" +
          "6,Nurse,100000,Jen\n" +
          "7,Teacher,80000,Maria\n" +
          "8,Drug Dealer,8000000000,Mimi\n" +
          "9,Programmer,250000,Jenny\n" +
          "10,Teacher,400000,Jerry")
      assert(result2.tableData == expectedResult2.tableData)

      // PART 3
      val update3: Map[String, String] = Map("name" -> "Jerry", "hobby" -> "Dancing")
      val result3: Table = Utils.hobbies.update(Field("name", _ == "Jerry"), update3)
      assert(result3.tableData == Utils.hobbies.tableData)

      PUNCTAJ += 4
    }
  }
}