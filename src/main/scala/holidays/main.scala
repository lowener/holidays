package holidays




object Main {
    def main(args: Array[String]) {
      args.map(x => println(x))
      var csvList: Array[Csv] = args.map(x => Parse_csv.readCsv(x))
      csvList.map(x => x.printName)
  }
}
