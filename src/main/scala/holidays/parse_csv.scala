package holidays

import scala.io.Source
import collection.breakOut // To avoid copy on Array = >list Conversion

object Parse_csv {

    // Combine csv if a value has a comma in it
    def combineListOnCommas(l: List[String], acc: String = ""): List[String] = l match {
        case x::tail if (x.startsWith("\"") && x.endsWith("\"")) =>
                x.stripPrefix("\"").stripSuffix("\"").trim :: combineListOnCommas(tail)
        case x::tail if (x.startsWith("\"")) => combineListOnCommas(tail, x.stripPrefix("\"").trim + ",")
        case x::tail if (x.endsWith("\"")) => (acc + "," + x.stripSuffix("\"").trim) :: combineListOnCommas(tail)
        case x::tail => combineListOnCommas(tail, acc + "," + x)
        case Nil => Nil
    }

    def readCsv(file: String, nameCsv: String) : Csv = {
      val lines : List[String] = Source.fromFile(file).getLines.toList
      val names: List[String] = lines.head.split(',').toList
      val data = lines.drop(1)
                      .map(x => combineListOnCommas(x.split(",").toList).padTo(names.length, "").toList)

      new Csv(nameCsv,
              names.map(x => x.drop(1).dropRight(1)),
              data)
    }
}
