import scala.collection.mutable.{ListBuffer, Map => MutableMap, Set => MutableSet}
type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {
  
  // TODO 1.0
  def header: List[String] = {
    if (tableData.isEmpty)
      List()
    else
      tableData.head.keys.toList
  }
  def data: Tabular = tableData
  def name: String = tableName


  // TODO 1.1
  override def toString: String = {
    val hdr: List[String] = header
    val headerRow: String = hdr.mkString(",")
    val lines: List[String] = tableData.map {
      row => val values: List[String] = hdr.map {
        column => row.getOrElse(column, "")
      }
        values.mkString(",")
    }
    val CVS: List[String] = headerRow +: lines
    CVS.mkString("\n")
  }

  // TODO 1.3
  def insert(row: Row): Table = {
    if (this.tableData.contains(row)) {
      this
    } else {
      val newTable: Tabular = tableData :+ row
      Table(this.name, newTable)
    }
  }

  // TODO 1.4
  def delete(row: Row): Table = {
    val updatedTable: Tabular = tableData.filter(currentRow =>
    currentRow != row)
    Table(this.tableName, updatedTable)
  }

  // TODO 1.5
  def sort(column: String, ascending: Boolean = true): Table = {
    val header = this.header
    val sorted = tableData.sortBy(row => row.getOrElse(column, ""))
    val sortedLines = if (ascending) {
      sorted
    } else {
      sorted.reverse
    }
    Table(this.tableName, sortedLines)
  }

  // TODO 1.6
  def select(columns: List[String]): Table = {
    val selectedColumns: Set[String] = columns.toSet
    val newTable: Tabular = tableData.map{ row =>
      val newRow: Row = row.filter{ case (key, _) =>
      selectedColumns.contains(key)}
      newRow
    }
    Table(this.tableName, newTable)
  }

  // TODO 1.7
  // Construiti headerele noi concatenand numele tabelei la headere
  def cartesianProduct(otherTable: Table): Table = {
    val newTable: Tabular = for {
      rowFirst <- this.tableData
      rowSecond <- otherTable.tableData
    } yield {
      val newRowFirst: Row = rowFirst.map{
        case (key, value) => (this.tableName + "." + key -> value)
      }
      val newRowSecond: Row = rowSecond.map{
        case (key, value) => (otherTable.tableName + "." + key -> value)
      }
      newRowFirst ++ newRowSecond
    }
    Table(this.tableName, newTable)
  }
  
  // TODO 1.8

  def join(other: Table)(col1: String, col2: String): Table = {
    val thisHeader = this.header
    val otherHeader = other.header
    val thisHeaderSet = thisHeader.toSet
    val otherColsToAdd = otherHeader.filter(h => h != col2 && !thisHeaderSet.contains(h))
    val finalHeader: List[String] = thisHeader ++ otherColsToAdd
    if (this.tableData.isEmpty && other.tableData.isEmpty) {
      return Table(this.tableName, Nil)
    }

    val map1: Map[String, List[Row]] = this.tableData
      .groupBy(_.get(col1)).collect { case (Some(key), rows) if key.nonEmpty => key -> rows }
    val map2: Map[String, List[Row]] = other.tableData
      .groupBy(_.get(col2)).collect { case (Some(key), rows) if key.nonEmpty => key -> rows }

    val matchedRows = ListBuffer[Row]()
    val leftOnlyRows = ListBuffer[Row]()
    val rightOnlyRows = ListBuffer[Row]()

    def mergeValues(colName: String, row1: Row, row2: Row): String = {
      val val1Opt = row1.get(colName);
      val val2Opt = if (colName == col1) row2.get(col2) else row2.get(colName)
      (val1Opt, val2Opt) match {
        case (Some(v1), Some(v2)) => if (v1 == v2) v1 else v1 + ";" + v2;
        case (Some(v1), None) => v1
        case (None, Some(v2)) => v2;
        case (None, None) => ""
      }
    }

    val processedTable1Indices = MutableSet[Int]()
    val processedTable2Indices = MutableSet[Int]()

    this.tableData.zipWithIndex.foreach { case (row1, index1) =>
      row1.get(col1).flatMap(key1 => map2.get(key1)) match {
        case Some(matchingRows2) =>
          matchingRows2.foreach { row2 =>
            val mergedRow = finalHeader.map(col => col -> mergeValues(col, row1, row2)).toMap
            matchedRows += mergedRow
          }
          processedTable1Indices += index1
        case None => ()
      }
    }

    this.tableData.zipWithIndex.foreach { case (row1, index1) =>
      if (!processedTable1Indices.contains(index1)) {
        val leftRow = finalHeader.map(col => col -> row1.getOrElse(col, "")).toMap
        leftOnlyRows += leftRow
      }
    }
    other.tableData.foreach { row2 =>
      val key2Opt = row2.get(col2)
      val isMatched = key2Opt.exists(key2 => map1.contains(key2))

      if (!isMatched) {
        val rightRow = finalHeader.map { col =>
          val value = if (col == col1) row2.getOrElse(col2, "") else row2.getOrElse(col, "")
          col -> value
        }.toMap
        rightOnlyRows += rightRow
      }
    }

    val finalTableData = (matchedRows.toList ++ leftOnlyRows.toList ++ rightOnlyRows.toList).distinct
    val finalTableDataOrdered = matchedRows.toList ++ leftOnlyRows.toList ++ rightOnlyRows.toList

    Table(this.tableName, finalTableDataOrdered)
  }

  // TODO 2.3
  def filter(f: FilterCond): Table = {
    val filteredData: Tabular = this.tableData.filter{ row =>
      val result: Option[Boolean] = f.eval(row)
      result.contains(true)
    }
    Table(this.tableName, filteredData)
  }
  
  // TODO 2.4
  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedData = tableData.map{ row =>
      if (f.eval(row).contains(true)) row ++ updates
      else row
    }
    Table(this.tableName, updatedData)
  }

  // TODO 3.5
  // Implement indexing
  def apply(index: Int): Row = {
    tableData(index)
  }
}

object Table {
  // TODO 1.2
  def fromCSV(csv: String): Table = {
    val lines: List[String] = csv.linesIterator.toList
    val header: List[String] = lines.head.split(",").toList
    val rows: List[String] = lines.tail
    val tableData: Tabular = rows.map{ line =>
      val values: List[String] = line.split(",").toList
      header.zip(values).toMap
    }
    Table("tableName", tableData)
  }

  // TODO 1.9
  def apply(name: String, s: String): Table = {
    val lines: List[String] = s.linesIterator.toList
    val header: List[String] = lines.head.split(",").map(_.trim).toList
    val tableLines: List[String] = lines.tail
    val tableRows: Tabular = tableLines.map{ row =>
      val values: List[String] = row.split(",").map(_.trim).toList
      header.zip(values).toMap
    }
    Table(name, tableRows)
  }
}