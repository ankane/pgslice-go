package pgslice

import (
  "fmt"
  "github.com/urfave/cli"
)

func Prep(ctx *cli.Context) error {
  column := ctx.Args().Get(1)
  period := ctx.Args().Get(2)

  partition := !ctx.Bool("no-partition")
  triggerBased := ctx.Bool("trigger-based")

  db, err := Connection(ctx)
  if err != nil {
    return err
  }

  table := CreateTable(ctx.Args().Get(0))
  intermediateTable := table.IntermediateTable()
  triggerName := table.TriggerName()

  if !partition {
    if column != "" || period != "" {
      return Abort("Usage: pgslice prep TABLE --no-partition")
    }
    if triggerBased {
      return Abort("Can't use --trigger-based and --no-partition")
    }
  }

  if !table.Exists(db) {
    return Abort(fmt.Sprintf("Table not found: %s", table.FullName()))
  }

  if intermediateTable.Exists(db) {
    return Abort(fmt.Sprintf("Table already exists: %s", intermediateTable.FullName()))
  }

  if partition {
    if column == "" || period == "" {
      return Abort("Usage: pgslice prep TABLE COLUMN PERIOD")
    }

    if !Contains(table.Columns(db), column) {
      return Abort(fmt.Sprintf("Column not found: %s", column))
    }

    validPeriods := []string{"day", "month", "year"}
    if !Contains(validPeriods, period) {
      return Abort("Invalid period: " + period)
    }
  }

  queries := []string{}

  serverVersionNum := ServerVersionNum(db)

  declarative := serverVersionNum >= 100000 && !triggerBased

  var indexDefs []string

  if declarative && partition {
    queries = append(queries, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE (%s);", QuoteTable(intermediateTable), QuoteTable(table), QuoteIdent(column)))

    if serverVersionNum >= 110000 {
      indexDefs = table.IndexDefs(db)

      for _, def := range indexDefs {
        queries = append(queries, MakeIndexDef(def, intermediateTable))
      }
    }

    // add comment
    cast := table.ColumnCast(db, column)
    queries = append(queries, fmt.Sprintf("COMMENT ON TABLE %s is 'column:%s,period:%s,cast:%s';", QuoteTable(intermediateTable), column, period, cast))
  } else {
    queries = append(queries, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL);", QuoteTable(intermediateTable), QuoteTable(table)))

    for _, def := range table.ForeignKeys(db) {
      queries = append(queries, MakeFkDef(def, intermediateTable))
    }
  }

  if partition && !declarative {
    queries = append(queries, fmt.Sprintf(`CREATE FUNCTION %s()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'Create partitions first.';
    END;
    $$ LANGUAGE plpgsql;`, QuoteIdent(triggerName)))

    queries = append(queries, fmt.Sprintf(`CREATE TRIGGER %s
    BEFORE INSERT ON %s
    FOR EACH ROW EXECUTE PROCEDURE %s();`, QuoteIdent(triggerName), QuoteTable(intermediateTable), QuoteIdent(triggerName)))

    cast := table.ColumnCast(db, column)

    queries = append(queries, fmt.Sprintf("COMMENT ON TRIGGER %s ON %s IS 'column:%s,period:%s,cast:%s';", QuoteIdent(triggerName), QuoteTable(intermediateTable), column, period, cast))
  }

  return RunQueries(db, queries, ctx)
}
