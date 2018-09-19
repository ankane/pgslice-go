package pgslice

import (
	"fmt"
	"github.com/urfave/cli"
	"sort"
	"strings"
	"time"
)

func AddPartitions(ctx *cli.Context) error {
	originalTable := CreateTable(ctx.Args().Get(0))

	table := originalTable
	if ctx.Bool("intermediate") {
		table = table.IntermediateTable()
	}
	triggerName := originalTable.TriggerName()

	db, err := Connection(ctx)
	if err != nil {
		return err
	}

	if !table.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", table.FullName()))
	}

	future := ctx.Int("future")
	past := ctx.Int("past")

	period, field, cast, declarative := FetchSettings(db, originalTable, table)
	if period == "" {
		message := fmt.Sprintf("No settings found: %s", table.FullName())
		if !ctx.Bool("intermediate") {
			message = message + "\nDid you mean to use --intermediate?"
		}
		return Abort(message)
	}

	queries := []string{}

	// today = utc date
	today := RoundDate(time.Now().UTC(), period)

	var schemaTable Table
	if !declarative {
		schemaTable = table
	} else if ctx.Bool("intermediate") {
		schemaTable = originalTable
	} else {
		partitions := originalTable.Partitions(db)
		schemaTable = partitions[len(partitions)-1]
	}

	// indexes automatically propagate in Postgres 11+
	indexDefs := []string{}
	if !declarative || ServerVersionNum(db) < 110000 {
		indexDefs = schemaTable.IndexDefs(db)
	}

	fkDefs := schemaTable.ForeignKeys(db)
	primaryKey := schemaTable.PrimaryKey(db)

	addedPartitions := []Table{}

	for i := past * -1; i <= future; i++ {
		day := AdvanceDate(today, period, i)

		nameFormat := day.Format(NameFormat(period))
		partition := Table{Schema: originalTable.Schema, Name: fmt.Sprintf("%s_%s", originalTable.Name, nameFormat)}
		// TODO use partitions
		if partition.Exists(db) {
			continue
		}
		addedPartitions = append(addedPartitions, partition)

		if declarative {
			queries = append(queries, fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s);", QuoteTable(partition), QuoteTable(table), SQLDate(day, cast, false), SQLDate(AdvanceDate(day, period, 1), cast, false)))
		} else {
			queries = append(queries, fmt.Sprintf(`CREATE TABLE %s
    (CHECK (%s >= %s AND %s < %s))
    INHERITS (%s);`, QuoteTable(partition), QuoteIdent(field), SQLDate(day, cast, true), QuoteIdent(field), SQLDate(AdvanceDate(day, period, 1), cast, true), QuoteTable(table)))
		}

		if len(primaryKey) > 0 {
			queries = append(queries, fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);", QuoteTable(partition), QuoteColumns(primaryKey)))
		}

		for _, def := range indexDefs {
			queries = append(queries, MakeIndexDef(def, partition))
		}

		for _, def := range fkDefs {
			queries = append(queries, MakeFkDef(def, partition))
		}
	}

	if !declarative {
		// update trigger based on existing partitions
		currentDefs := []string{}
		futureDefs := []string{}
		pastDefs := []string{}
		nameFormat := NameFormat(period)
		partitions := append(originalTable.Partitions(db), addedPartitions...)

		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Name < partitions[j].Name
		})

		for _, partition := range partitions {
			day := PartitionDate(partition, nameFormat)

			sql := fmt.Sprintf(`(NEW.%s >= %s AND NEW.%s < %s) THEN
            INSERT INTO %s VALUES (NEW.*);`, QuoteIdent(field), SQLDate(day, cast, true), QuoteIdent(field), SQLDate(AdvanceDate(day, period, 1), cast, true), QuoteTable(partition))

			if day.Before(today) {
				pastDefs = append(pastDefs, sql)
			} else if AdvanceDate(day, period, 1).Before(today) {
				currentDefs = append(currentDefs, sql)
			} else {
				futureDefs = append(futureDefs, sql)
			}
		}

		// order by current period, future periods asc, past periods desc
		// TODO reverse past defs
		triggerDefs := append(currentDefs, futureDefs...)
		triggerDefs = append(triggerDefs, pastDefs...)

		if len(triggerDefs) > 0 {
			queries = append(queries, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
    RETURNS trigger AS $$
    BEGIN
        IF %s
        ELSE
            RAISE EXCEPTION 'Date out of range. Ensure partitions are created.';
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;`, QuoteIdent(triggerName), strings.Join(triggerDefs, "\n        ELSIF ")))
		}
	}

	if len(queries) == 0 {
		return nil
	}

	return RunQueries(db, queries, ctx)
}
