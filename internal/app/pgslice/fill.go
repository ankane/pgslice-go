package pgslice

import (
	"fmt"
	"math"
	"time"

	"github.com/urfave/cli"
)

func Fill(ctx *cli.Context) error {
	table := CreateTable(ctx.Args().Get(0))
	swapped := ctx.Bool("swapped")
	sleep := ctx.Int("sleep")

	var sourceTable Table
	if ctx.String("source-table") != "" {
		sourceTable = CreateTable(ctx.String("source-table"))
	}
	var destTable Table
	if ctx.String("dest-table") != "" {
		destTable = CreateTable(ctx.String("dest-table"))
	}

	if swapped {
		if sourceTable == (Table{}) {
			sourceTable = table.RetiredTable()
		}
		if destTable == (Table{}) {
			destTable = table
		}
	} else {
		if sourceTable == (Table{}) {
			sourceTable = table
		}
		if destTable == (Table{}) {
			destTable = table.IntermediateTable()
		}
	}

	db, err := Connection(ctx)
	if err != nil {
		return err
	}

	if !sourceTable.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", sourceTable.FullName()))
	}

	if !destTable.Exists(db) {
		return Abort(fmt.Sprintf("Table not found: %s", destTable.FullName()))
	}

	period, field, cast, declarative := FetchSettings(db, table, destTable)

	var startingTime time.Time
	var endingTime time.Time
	if period != "" {
		nameFormat := NameFormat(period)

		// TODO add period
		partitions := table.Partitions(db)
		if len(partitions) > 0 {
			startingTime = PartitionDate(partitions[0], nameFormat)
			endingTime = AdvanceDate(PartitionDate(partitions[len(partitions)-1], nameFormat), period, 1)
		}
	}

	schemaTable := table
	if period != "" && declarative {
		partitions := destTable.Partitions(db)
		if len(partitions) == 0 {
			return Abort("No partitions")
		}
		schemaTable = partitions[len(partitions)-1]
	}

	primaryKey := schemaTable.PrimaryKey(db)
	if len(primaryKey) == 0 {
		return Abort("No primary key")
	}
	primaryKeyColumn := primaryKey[0]

	maxSourceID := sourceTable.MaxID(db, primaryKeyColumn, "", -1)

	maxDestID := 0
	if ctx.Int("start") > 0 {
		maxDestID = ctx.Int("start")
	} else if swapped {
		maxDestID = destTable.MaxID(db, primaryKeyColumn, ctx.String("where"), maxSourceID)
	} else {
		maxDestID = destTable.MaxID(db, primaryKeyColumn, ctx.String("where"), -1)
	}

	if maxDestID == 0 && !swapped {
		minSourceID := sourceTable.MinID(db, primaryKeyColumn, field, cast, startingTime, ctx.String("where"))
		maxDestID = minSourceID - 1
	}

	startingID := maxDestID
	fields := QuoteColumns(sourceTable.Columns(db))

	batchSize := ctx.Int("batch-size")

	i := 1
	batchCount := int(math.Ceil(float64(maxSourceID-startingID) / float64(batchSize)))

	if batchCount == 0 {
		LogSQL("/* nothing to fill */")
	}

	for ; startingID < maxSourceID; startingID += batchSize {
		where := fmt.Sprintf("%s > %d AND %s <= %d", QuoteIdent(primaryKeyColumn), startingID, QuoteIdent(primaryKeyColumn), startingID+batchSize)

		if startingTime != (time.Time{}) {
			where = where + fmt.Sprintf(" AND %s >= %s AND %s < %s", QuoteIdent(field), SQLDate(startingTime, cast, true), QuoteIdent(field), SQLDate(endingTime, cast, true))
		}

		if ctx.String("where") != "" {
			where = where + " AND " + ctx.String("where")
		}

		query := fmt.Sprintf(`/* %d of %d */
INSERT INTO %s (%s)
    SELECT %s FROM %s
    WHERE %s`, i, batchCount, QuoteTable(destTable), fields, fields, QuoteTable(sourceTable), where)

		err := RunQuery(db, query, ctx)
		if err != nil {
			return err
		}

		i++

		if sleep > 0 && startingID < maxSourceID {
			time.Sleep(time.Duration(sleep) * time.Second)
		}
	}

	return nil
}
