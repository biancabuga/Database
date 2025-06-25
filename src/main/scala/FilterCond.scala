import scala.language.implicitConversions

trait FilterCond {
  def eval(r: Row): Option[Boolean]

  // TODO 2.2
  def ===(other: FilterCond): FilterCond = Equal(this, other)
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  def unary_! : FilterCond = Not(this)
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName).map(predicate)
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    if (conditions.isEmpty) {
      None
    } else {
      val results: List[Option[Boolean]] = conditions.map(_.eval(r))
      if (results.exists(_.isEmpty)) {
        None
      } else {
        val booleanResults: List[Boolean] = results.flatten
        if (booleanResults.isEmpty) {
          None
        } else {
          booleanResults.reduceOption(op)
        }
      }
    }
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    f.eval(r).map(!_)
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = All(List(f1, f2))
def Or(f1: FilterCond, f2: FilterCond): FilterCond = Any(List(f1, f2))
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = Eq(f1, f2)

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    def opOr(opt1: Option[Boolean], opt2: Option[Boolean]): Option[Boolean] = {
      (opt1, opt2) match {
        case (Some(true), _) | (_, Some(true)) => Some(true)
        case (None, _) | (_, None) => None
        case (Some(false), Some(false)) => Some(false)
      }
    }

    fs.map(_.eval(r)).foldLeft(Option(false))(opOr)
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    def opAnd(opt1: Option[Boolean], opt2: Option[Boolean]): Option[Boolean] = {
      (opt1, opt2) match {
        case (Some(false), _) | (_, Some(false)) => Some(false)
        case (None, _) | (_, None) => None
        case (Some(true), Some(true)) => Some(true)
      }
    }

    fs.map(_.eval(r)).foldLeft(Option(true))(opAnd)
  }
}

case class Eq(f1: FilterCond, f2: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val res1 = f1.eval(r)
    val res2 = f2.eval(r)
    (res1, res2) match {
      case (Some(b1), Some(b2)) => Some(b1 == b2)
      case _ => None
    }
  }
}