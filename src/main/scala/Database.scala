case class Database(tables: List[Table]) {
  // TODO 3.0
  override def toString: String = {
    if (tables.isEmpty) {
      "Database(Empty)"
    } else {
      val summaryLines = tables.map { t =>
        "  - " + t.name + " (" + t.tableData.size + " rows)"
      }
      "Database containing " + tables.size + " tables:\n" + summaryLines.mkString("\n")
    }
  }

  // TODO 3.1
  def insert(tableName: String): Database = {
    val nameExists = tables.exists(_.name == tableName)
    if (nameExists) {
      this
    } else {
      val newTable = Table(tableName, Nil)
      Database(tables :+ newTable)
    }
  }

  // TODO 3.2
  def update(tableName: String, newTable: Table): Database = {
    val tableIndex = tables.indexWhere(_.name == tableName)
    if (tableIndex == -1) {
      this
    } else {
      val updatedTableWithName = newTable.copy(tableName = tableName)
      val updatedTables = tables.updated(tableIndex, updatedTableWithName)
      Database(updatedTables)
    }
  }
  // TODO 3.3
  def delete(tableName: String): Database = {
    val updatedTables = tables.filterNot(_.name == tableName)
    if (updatedTables.size == tables.size) {
      this
    } else {
      Database(updatedTables)
    }
  }

  // TODO 3.4
  def selectTables(tableNames: List[String]): Option[Database] = {
    val selectedTables = tables.filter(table => tableNames.contains(table.name))
    val foundNamesSet = selectedTables.map(_.name).toSet
    val requestedNamesSet = tableNames.toSet
    if (foundNamesSet == requestedNamesSet) {
      Some(Database(selectedTables))
    } else {
      None
    }
  }

  // TODO 3.5
  // Implement indexing here
  def apply(index: Int): Table = {
    tables(index)
  }

  def apply(tableName: String): Table = {
    tables.find(_.name == tableName) match {
      case Some(table) => table
      case None => throw new NoSuchElementException("Table '" + tableName + "' not found in the database.")
    }
  }
}
