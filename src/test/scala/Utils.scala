object Utils {
  val people : Table = Table(
    "People",
    List(
      Map("id" -> "1", "name" -> "John", "age" -> "23", "address" -> "123 Main St"),
      Map("id" -> "2", "name" -> "Jane", "age" -> "27", "address" -> "456 Elm St"),
      Map("id" -> "3", "name" -> "Joe", "age" -> "30", "address" -> "789 Maple St"),
      Map("id" -> "4", "name" -> "Jill", "age" -> "25", "address" -> "101 Oak St"),
      Map("id" -> "5", "name" -> "Jack", "age" -> "27", "address" -> "112 Pine St"),
      Map("id" -> "6", "name" -> "Jen", "age" -> "24", "address" -> "131 Cedar St"),
      Map("id" -> "7", "name" -> "Jim", "age" -> "26", "address" -> "141 Birch St"),
      Map("id" -> "8", "name" -> "Jesse", "age" -> "29", "address" -> "151 Spruce St"),
      Map("id" -> "9", "name" -> "Jenny", "age" -> "23", "address" -> "161 Fir St"),
      Map("id" -> "10", "name" -> "Jerry", "age" -> "28", "address" -> "171 Larch St")
    )
  )

  val peopleStr: String =
    """|id,name,age,address
       |1,John,23,123 Main St
       |2,Jane,27,456 Elm St
       |3,Joe,30,789 Maple St
       |4,Jill,25,101 Oak St
       |5,Jack,27,112 Pine St
       |6,Jen,24,131 Cedar St
       |7,Jim,26,141 Birch St
       |8,Jesse,29,151 Spruce St
       |9,Jenny,23,161 Fir St
       |10,Jerry,28,171 Larch St""".stripMargin
  
  val animals : Table = Table("Animals",
  List(
    Map("Species" -> "Dog", "Name" -> "Bella", "Age" -> "3", "Color" -> "Brown"),
    Map("Species" -> "Rabbit", "Name" -> "Lola", "Age" -> "1", "Color" -> "Black"),
    Map("Species" -> "Fish", "Name" -> "Nemo", "Age" -> "1", "Color" -> "Orange"),
    Map("Species" -> "Bird", "Name" -> "Tweety", "Age" -> "2", "Color" -> "Yellow"),
    Map("Species" -> "Lizard", "Name" -> "Ziggy", "Age" -> "4", "Color" -> "Green"),
    Map("Species" -> "Turtle", "Name" -> "Tina", "Age" -> "5", "Color" -> "Brown"),
    Map("Species" -> "Hamster", "Name" -> "Hammy", "Age" -> "1", "Color" -> "Grey"),
    Map("Species" -> "Rabbit", "Name" -> "Luna", "Age" -> "2", "Color" -> "White"),
    Map("Species" -> "Frog", "Name" -> "Frodo", "Age" -> "2", "Color" -> "Green"),
    Map("Species" -> "Snake", "Name" -> "Slytherin", "Age" -> "3", "Color" -> "Black")
    )
  )

  val animalsStr: String =
    """|Species,Name,Age,Color
       |Dog,Bella,3,Brown
       |Rabbit,Lola,1,Black
       |Fish,Nemo,1,Orange
       |Bird,Tweety,2,Yellow
       |Lizard,Ziggy,4,Green
       |Turtle,Tina,5,Brown
       |Hamster,Hammy,1,Grey
       |Rabbit,Luna,2,White
       |Frog,Frodo,2,Green
       |Snake,Slytherin,3,Black""".stripMargin

  val jobs : Table = Table(
    "Jobs",
    List(
      Map("id" -> "1", "title" -> "Engineer", "salary" -> "100000", "person_name" -> "John"),
      Map("id" -> "2", "title" -> "Manager", "salary" -> "200000", "person_name" -> "Jane"),
      Map("id" -> "3", "title" -> "CEO", "salary" -> "300000", "person_name" -> "Joe"),
      Map("id" -> "4", "title" -> "Banker", "salary" -> "150000", "person_name" -> "Mona"),
      Map("id" -> "5", "title" -> "Doctor", "salary" -> "200000", "person_name" -> "Jack"),
      Map("id" -> "6", "title" -> "Nurse", "salary" -> "100000", "person_name" -> "Jen"),
      Map("id" -> "7", "title" -> "Teacher", "salary" -> "80000", "person_name" -> "Maria"),
      Map("id" -> "8", "title" -> "Engineer", "salary" -> "120000", "person_name" -> "Mimi"),
      Map("id" -> "9", "title" -> "Programmer", "salary" -> "250000", "person_name" -> "Jenny"),
      Map("id" -> "10", "title" -> "Teacher", "salary" -> "400000", "person_name" -> "Jerry")
    )
  )

  val jobsStr: String =
    """|id,title,salary,person_name
       |1,Engineer,100000,John
       |2,Manager,200000,Jane
       |3,CEO,300000,Joe
       |4,Banker,150000,Mona
       |5,Doctor,200000,Jack
       |6,Nurse,100000,Jen
       |7,Teacher,80000,Maria
       |8,Engineer,120000,Mimi
       |9,Programmer,250000,Jenny
       |10,Teacher,400000,Jerry""".stripMargin


  val ob:Table = Table(
    "ob",
    List(
      Map("id" -> "1", "name" -> "John", "age" -> "21", "address" -> "123 Main St"),
      Map("id" -> "2", "name" -> "Jane", "age" -> "27", "address" -> "456 Elm St"),
      Map("id" -> "3", "name" -> "Joe", "age" -> "30", "address" -> "789 Maple St"),
      Map("id" -> "4", "name" -> "Jill", "age" -> "22", "address" -> "101 Oak St"),
      Map("id" -> "5", "name" -> "Jack", "age" -> "53", "address" -> "112 Pine St"),
      Map("id" -> "6", "name" -> "Jen", "age" -> "24", "address" -> "131 Cedar St"),
      Map("id" -> "7", "name" -> "Jim", "age" -> "45", "address" -> "141 Birch St"),
      Map("id" -> "8", "name" -> "Jesse", "age" -> "29", "address" -> "151 Spruce St") 
    )
  )

  val obStr: String =
    """|id,name,age,address
       |1,John,21,123 Main St
       |2,Jane,27,456 Elm St
       |3,Joe,30,789 Maple St
       |4,Jill,22,101 Oak St
       |5,Jack,53,112 Pine St
       |6,Jen,24,131 Cedar St
       |7,Jim,45,141 Birch St
       |8,Jesse,29,151 Spruce St""".stripMargin

  

  val hobbies: Table = Table(
    "Hobbies",
    List(
      Map("id" -> "1", "name" -> "John", "hobby" -> "Basketball"),
      Map("id" -> "2", "name" -> "Jane", "hobby" -> "Skiing"),
      Map("id" -> "3", "name" -> "Joe", "hobby" -> "Football"),
      Map("id" -> "4", "name" -> "Mona", "hobby" -> "Painting"),
      Map("id" -> "5", "name" -> "Jack", "hobby" -> "Clay modeling"),
      Map("id" -> "6", "name" -> "Jen", "hobby" -> "Film making"),
      Map("id" -> "7", "name" -> "Maria", "hobby" -> "Skin care"),
      Map("id" -> "8", "name" -> "Mimi", "hobby" -> "Music"),
      Map("id" -> "9", "name" -> "Jenny", "hobby" -> "Makeup"),
      Map("id" -> "10", "name" -> "Jerry", "hobby" -> "Dancing")
    )

  )

  val hobbiesStr: String =
    """|id,name,hobby
       |1,John,Basketball
       |2,Jane,Skiing
       |3,Joe,Football
       |4,Mona,Painting
       |5,Jack,Clay modeling
       |6,Jen,Film making
       |7,Maria,Skin care
       |8,Mimi,Music
       |9,Jenny,Makeup
       |10,Jerry,Dancing""".stripMargin

  val countries: Table = Table(
        "Countries",
        List(
          Map("id" -> "1", "country" -> "Romania"),
          Map("id" -> "2", "country" -> "France"),
          Map("id" -> "3", "country" -> "Germany"),
          Map("id" -> "4", "country" -> "Italy"),
          Map("id" -> "5", "country" -> "Spain"),
          Map("id" -> "6", "country" -> "Canada"),
          Map("id" -> "7", "country" -> "Japan"),
          Map("id" -> "8", "country" -> "Australia"),
          Map("id" -> "9", "country" -> "Brazil"),
          Map("id" -> "10", "country" -> "USA")
        )
      )
  val countriesStr: String =
        """|id,country
           |1,Romania
           |2,France
           |3,Germany
           |4,Italy
           |5,Spain
           |6,Canada
           |7,Japan
           |8,Australia
           |9,Brazil
           |10,USA""".stripMargin
           
           
	
  val cities: Table = Table(
        "Cities",
        List(
          Map("id_city" -> "1", "city" -> "Bucharest", "country_id" -> "1"),
          Map("id_city" -> "2", "city" -> "Cluj", "country_id" -> "1"),
          Map("id_city" -> "3", "city" -> "Paris", "country_id" -> "2"),
          Map("id_city" -> "4", "city" -> "Berlin", "country_id" -> "3"),
          Map("id_city" -> "5", "city" -> "Rome", "country_id" -> "4"),
          Map("id_city" -> "6", "city" -> "Barcelona", "country_id" -> "5"),
          Map("id_city" -> "7", "city" -> "Toronto", "country_id" -> "6"),
          Map("id_city" -> "8", "city" -> "Tokyo", "country_id" -> "7"),
          Map("id_city" -> "9", "city" -> "Sydney", "country_id" -> "8"),
          Map("id_city" -> "10", "city" -> "New York", "country_id" -> "10")
        )
      )
	
  val citiesStr: String =
  """|id_city,city,country_id
     |1,Bucharest,1
     |2,Cluj,1
     |3,Paris,2
     |4,Berlin,3
     |5,Rome,4
     |6,Barcelona,5
     |7,Toronto,6
     |8,Tokyo,7
     |9,Sydney,8
     |10,New York,10""".stripMargin

  val countriesMini: Table = Table(
    "Countries",
    List(
      Map("id" -> "1", "country" -> "Romania"),
      Map("id" -> "2", "country" -> "France"),
      Map("id" -> "3", "country" -> "Germany"),
    )
  )
  val countriesMiniStr: String =
    """|id,country
       |1,Romania
       |2,France
       |3,Germany""".stripMargin


  val citiesMini: Table = Table(
    "Cities",
    List(
      Map("id_city" -> "1", "city" -> "Bucharest", "country_id" -> "1"),
      Map("id_city" -> "2", "city" -> "Paris", "country_id" -> "2"),
      Map("id_city" -> "3", "city" -> "Berlin", "country_id" -> "3"),
      )
  )

  val citiesMiniStr: String =
    """|id_city,city,country_id
       |1,Bucharest,1
       |2,Paris,2
       |3,Berlin,3""".stripMargin

  val customers: Table = Table(
    "Customers",
    List(
      Map("id" -> "1", "age" -> "27", "city" -> "Bucharest"),
      Map("id" -> "2", "age" -> "31", "city" -> "Cluj"),
      Map("id" -> "3", "age" -> "30", "city" -> "Paris"),
      Map("id" -> "4", "age" -> "28", "city" -> "Berlin"),
      Map("id" -> "5", "age" -> "33", "city" -> "Rome"),
      Map("id" -> "6", "age" -> "26", "city" -> "Barcelona"),
      Map("id" -> "7", "age" -> "35", "city" -> "Toronto"),
      Map("id" -> "8", "age" -> "25", "city" -> "Tokyo"),
      Map("id" -> "9", "age" -> "29", "city" -> "Sydney"),
      Map("id" -> "10", "age" -> "32", "city" -> "New York"),
      Map("id" -> "11", "age" -> "27", "city" -> "Cluj"),
      Map("id" -> "12", "age" -> "34", "city" -> "Paris"),
      Map("id" -> "13", "age" -> "30", "city" -> "Barcelona"),
      Map("id" -> "14", "age" -> "26", "city" -> "Berlin"),
      Map("id" -> "15", "age" -> "28", "city" -> "Toronto"),
      Map("id" -> "16", "age" -> "31", "city" -> "New York"),
      Map("id" -> "17", "age" -> "33", "city" -> "Rome"),
      Map("id" -> "18", "age" -> "25", "city" -> "Tokyo"),
      Map("id" -> "19", "age" -> "35", "city" -> "Bucharest"),
      Map("id" -> "20", "age" -> "29", "city" -> "Sydney")
    )
  )

  val customersStr: String =
    """|id,age,city
       |1,27,Bucharest
       |2,31,Cluj
       |3,30,Paris
       |4,28,Berlin
       |5,33,Rome
       |6,26,Barcelona
       |7,35,Toronto
       |8,25,Tokyo
       |9,29,Sydney
       |10,32,New York
       |11,27,Cluj
       |12,34,Paris
       |13,30,Barcelona
       |14,26,Berlin
       |15,28,Toronto
       |16,31,New York
       |17,33,Rome
       |18,25,Tokyo
       |19,35,Bucharest
       |20,29,Sydney""".stripMargin


  val orders: Table = Table(
    "Orders",
    List(
      Map("order_id" -> "1", "date" -> "2013-03-15", "cost" -> "45", "employee_id" -> "1", "customer_id" -> "1"),
      Map("order_id" -> "2", "date" -> "2017-07-22", "cost" -> "78", "employee_id" -> "2", "customer_id" -> "2"),
      Map("order_id" -> "3", "date" -> "2020-11-05", "cost" -> "36", "employee_id" -> "3", "customer_id" -> "3"),
      Map("order_id" -> "4", "date" -> "2014-01-10", "cost" -> "92", "employee_id" -> "4", "customer_id" -> "4"),
      Map("order_id" -> "5", "date" -> "2016-05-30", "cost" -> "58", "employee_id" -> "5", "customer_id" -> "5"),
      Map("order_id" -> "6", "date" -> "2012-09-09", "cost" -> "66", "employee_id" -> "1", "customer_id" -> "6"),
      Map("order_id" -> "7", "date" -> "2022-12-25", "cost" -> "39", "employee_id" -> "2", "customer_id" -> "7"),
      Map("order_id" -> "8", "date" -> "2019-08-14", "cost" -> "84", "employee_id" -> "3", "customer_id" -> "8"),
      Map("order_id" -> "9", "date" -> "2015-04-01", "cost" -> "25", "employee_id" -> "4", "customer_id" -> "9"),
      Map("order_id" -> "10", "date" -> "2021-10-20", "cost" -> "99", "employee_id" -> "5", "customer_id" -> "10"),
      Map("order_id" -> "11", "date" -> "2018-06-18", "cost" -> "72", "employee_id" -> "1", "customer_id" -> "11"),
      Map("order_id" -> "12", "date" -> "2013-02-07", "cost" -> "63", "employee_id" -> "2", "customer_id" -> "12"),
      Map("order_id" -> "13", "date" -> "2020-03-11", "cost" -> "41", "employee_id" -> "3", "customer_id" -> "13"),
      Map("order_id" -> "14", "date" -> "2014-07-04", "cost" -> "55", "employee_id" -> "4", "customer_id" -> "14"),
      Map("order_id" -> "15", "date" -> "2016-01-29", "cost" -> "77", "employee_id" -> "5", "customer_id" -> "15"),
      Map("order_id" -> "16", "date" -> "2017-09-12", "cost" -> "31", "employee_id" -> "1", "customer_id" -> "16"),
      Map("order_id" -> "17", "date" -> "2012-10-10", "cost" -> "88", "employee_id" -> "2", "customer_id" -> "17"),
      Map("order_id" -> "18", "date" -> "2022-11-23", "cost" -> "20", "employee_id" -> "3", "customer_id" -> "18"),
      Map("order_id" -> "19", "date" -> "2019-05-05", "cost" -> "90", "employee_id" -> "4", "customer_id" -> "19"),
      Map("order_id" -> "20", "date" -> "2021-06-30", "cost" -> "53", "employee_id" -> "5", "customer_id" -> "20")
    )
  )


  val ordersStr: String =
    """|order_id,date,cost,employee_id,customer_id
       |1,2013-03-15,45,1,1
       |2,2017-07-22,78,2,2
       |3,2020-11-05,36,3,3
       |4,2014-01-10,92,4,4
       |5,2016-05-30,58,5,5
       |6,2012-09-09,66,1,6
       |7,2022-12-25,39,2,7
       |8,2019-08-14,84,3,8
       |9,2015-04-01,25,4,9
       |10,2021-10-20,99,5,10
       |11,2018-06-18,72,1,11
       |12,2013-02-07,63,2,12
       |13,2020-03-11,41,3,13
       |14,2014-07-04,55,4,14
       |15,2016-01-29,77,5,15
       |16,2017-09-12,31,1,16
       |17,2012-10-10,88,2,17
       |18,2022-11-23,20,3,18
       |19,2019-05-05,90,4,19
       |20,2021-06-30,53,5,20""".stripMargin



  val db1 = Database(
    List(people, jobs, hobbies)
  )

  val db2 = Database(
    List(
      Table("1", List()),
      Table("2", List()),
      Table("3", List()),
      Table("4", List()),
      Table("5", List()),
      Table("6", List()),
      Table("7", List()),
      Table("8", List()),
      Table("9", List()),
      Table("10", List())
    ))
  val db3 = Database(
    List(customers, orders)
  )
}


