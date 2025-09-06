package cmd

import (
	"fmt"

	"github.com/urfave/cli"
)

func Unprep(ctx *cli.Context) error {
	table := CreateTable(ctx.Args().Get(0))
	intermediateTable := table.IntermediateTable()
	triggerName := table.TriggerName()

	db, err := Connection(ctx)
	if err != nil {
		return err
	}

	exists, err := intermediateTable.Exists(db)
	if err != nil {
		return err
	}
	if !exists {
		return Abort(fmt.Sprintf("Table not found: %s", intermediateTable.FullName()))
	}

	queries := []string{fmt.Sprintf("DROP TABLE %s CASCADE;", QuoteTable(intermediateTable)), fmt.Sprintf("DROP FUNCTION IF EXISTS %s();", QuoteIdent(triggerName))}

	return RunQueries(db, queries, ctx)
}
