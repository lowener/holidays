package holidays

import scala.io.Source

object Parse_csv {
    def readCsv(file: String) : Csv = {
      val lines : List[String] = Source.fromFile(file).getLines.toList
      val names: List[String] = lines.head.split(',').toList
      new Csv(names.map(x => x.drop(1).dropRight(1)),
              lines.drop(1).map(x =>
                x.split(',')
                 .map(el => el.stripPrefix("\"").stripSuffix("\"").trim)
                 .toList))
    }
}
