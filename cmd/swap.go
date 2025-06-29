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

	if !table.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", table.FullName()))
	}

	if !intermediateTable.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", intermediateTable.FullName()))
	}

	if retiredTable.Exists(db) {
		return Abort(fmt.Sprintf("Table already exists: %s", retiredTable.FullName()))
	}

	queries := []string{fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(table), QuoteNoSchema(retiredTable)), fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(intermediateTable), QuoteNoSchema(table))}

	for _, sequence := range table.Sequences(db) {
		queries = append(queries, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s;", QuoteIdent(sequence.Name), QuoteTable(table), QuoteIdent(sequence.Column)))
	}

	if ServerVersionNum(db) >= 90300 {
		queries = append([]string{fmt.Sprintf("SET LOCAL lock_timeout = '%s';", lockTimeout)}, queries...)
	}

	return RunQueries(db, queries, ctx)
}
