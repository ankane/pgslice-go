package cmd

import (
	"fmt"

	"github.com/urfave/cli"
)

func Analyze(ctx *cli.Context) error {
	table := CreateTable(ctx.Args().Get(0))
	swapped := ctx.Bool("swapped")

	parentTable := table
	if swapped {
		parentTable = table.IntermediateTable()
	}

	db, err := Connection(ctx)
	if err != nil {
		return err
	}

	partitions, err := table.Partitions(db)
	if err != nil {
		return err
	}
	analyzeList := append(partitions, parentTable)

	queries := make([]string, len(analyzeList))
	for i, t := range analyzeList {
		queries[i] = fmt.Sprintf("ANALYZE VERBOSE %s;", QuoteTable(t))
	}

	return RunQueriesWithoutTransaction(db, queries, ctx)
}
