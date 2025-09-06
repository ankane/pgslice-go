package cmd

import (
	"fmt"

	"github.com/urfave/cli"
)

func Swap(ctx *cli.Context) error {
	table := CreateTable(ctx.Args().Get(0))
	intermediateTable := table.IntermediateTable()
	retiredTable := table.RetiredTable()

	db, err := Connection(ctx)
	if err != nil {
		return err
	}
	lockTimeout := ctx.String("lock-timeout")

	exists, err := table.Exists(db)
	if err != nil {
		return err
	}
	if !exists {
		return Abort(fmt.Sprintf("Table not found: %s", table.FullName()))
	}

	exists, err = intermediateTable.Exists(db)
	if err != nil {
		return err
	}
	if !exists {
		return Abort(fmt.Sprintf("Table not found: %s", intermediateTable.FullName()))
	}

	exists, err = retiredTable.Exists(db)
	if err != nil {
		return err
	}
	if exists {
		return Abort(fmt.Sprintf("Table already exists: %s", retiredTable.FullName()))
	}

	queries := []string{fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(table), QuoteNoSchema(retiredTable)), fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(intermediateTable), QuoteNoSchema(table))}

	sequences, err := table.Sequences(db)
	if err != nil {
		return err
	}

	for _, sequence := range sequences {
		queries = append(queries, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s;", QuoteIdent(sequence.Name), QuoteTable(table), QuoteIdent(sequence.Column)))
	}

	serverVersionNum, err := ServerVersionNum(db)
	if err != nil {
		return err
	}

	if serverVersionNum >= 90300 {
		queries = append([]string{fmt.Sprintf("SET LOCAL lock_timeout = '%s';", lockTimeout)}, queries...)
	}

	return RunQueries(db, queries, ctx)
}
