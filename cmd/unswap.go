package cmd

import (
	"fmt"

	"github.com/urfave/cli"
)

func Unswap(ctx *cli.Context) error {
	table := CreateTable(ctx.Args().Get(0))
	intermediateTable := table.IntermediateTable()
	retiredTable := table.RetiredTable()

	db, err := Connection(ctx)
	if err != nil {
		return err
	}

	if !table.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", table.FullName()))
	}

	if !retiredTable.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", retiredTable.FullName()))
	}

	if intermediateTable.Exists(db) {
		return Abort(fmt.Sprintf("Table already exists: %s", intermediateTable.FullName()))
	}

	queries := []string{fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(table), QuoteNoSchema(intermediateTable)), fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", QuoteTable(retiredTable), QuoteNoSchema(table))}

	for _, sequence := range table.Sequences(db) {
		queries = append(queries, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s;", QuoteIdent(sequence.Name), QuoteTable(table), QuoteIdent(sequence.Column)))
	}

	return RunQueries(db, queries, ctx)
}
