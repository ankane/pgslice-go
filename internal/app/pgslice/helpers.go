package pgslice

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/urfave/cli"
	"os"
	"regexp"
	"strings"
	"time"
)

func CreateTable(name string) Table {
	schema := "public"
	if strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		schema = parts[0]
		name = parts[1]
	}
	return Table{Schema: schema, Name: name}
}

func Connection(ctx *cli.Context) (*sqlx.DB, error) {
	url := ctx.String("url")
	if url == "" {
		url = os.Getenv("PGSLICE_URL")
	}
	return sqlx.Connect("postgres", url)
}

func ServerVersionNum(db *sqlx.DB) int {
	var num int
	db.Get(&num, "SHOW server_version_num")
	return num
}

func QuoteTable(table Table) string {
	return strings.Join([]string{QuoteIdent(table.Schema), QuoteIdent(table.Name)}, ".")
}

func QuoteColumns(key []string) string {
	quotedKey := make([]string, len(key))
	for i, k := range key {
		quotedKey[i] = QuoteIdent(k)
	}
	return strings.Join(quotedKey, ", ")
}

func QuoteIdent(column string) string {
	return pq.QuoteIdentifier(column)
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func RoundDate(t time.Time, period string) time.Time {
	if period == "day" {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	} else if period == "month" {
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
}

func NameFormat(period string) string {
	if period == "day" {
		return "20060102"
	} else if period == "month" {
		return "200601"
	}
	return "2006"
}

func SQLDate(time time.Time, cast string, addCast bool) string {
	strFmt := "2006-01-02"
	if cast == "timestamptz" {
		strFmt = "2016-01-02 15:04:05 UTC"
	}
	str := fmt.Sprintf("'%s'", time.Format(strFmt))
	if addCast {
		return fmt.Sprintf("%s::%s", str, cast)
	}
	return str
}

func AdvanceDate(date time.Time, period string, count int) time.Time {
	if period == "day" {
		return date.AddDate(0, 0, count)
	} else if period == "month" {
		return date.AddDate(0, count, 0)
	} else {
		return date.AddDate(count, 0, 0)
	}
}

func QuoteNoSchema(table Table) string {
	return QuoteIdent(table.Name)
}

func Abort(message string) error {
	return cli.NewExitError(message, 1)
}

func RunQueries(db *sqlx.DB, queries []string, ctx *cli.Context) error {
	queries = append([]string{"BEGIN;"}, queries...)
	queries = append(queries, "COMMIT;")
	return RunQueriesWithoutTransaction(db, queries, ctx)
}

func LogSQL(s string) {
	fmt.Println(s)
}

func RunQueriesWithoutTransaction(db *sqlx.DB, queries []string, ctx *cli.Context) error {
	for _, query := range queries {
		err := RunQuery(db, query, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func RunQuery(db *sqlx.DB, query string, ctx *cli.Context) error {
	LogSQL(query)
	LogSQL("")
	if !ctx.Bool("dry-run") {
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func MakeIndexDef(def string, table Table) string {
	re1 := regexp.MustCompile(` ON \S+ USING `)
	re2 := regexp.MustCompile(` INDEX .+ ON `)
	def = re1.ReplaceAllString(def, fmt.Sprintf(" ON %s USING ", QuoteTable(table)))
	def = re2.ReplaceAllString(def, " INDEX ON ")
	return def + ";"
}

func MakeFkDef(def string, table Table) string {
	return "ALTER TABLE " + QuoteTable(table) + " ADD " + def + ";"
}

func PartitionDate(partition Table, nameFormat string) time.Time {
	parts := strings.Split(partition.Name, "_")
	day, _ := time.Parse(nameFormat, parts[len(parts)-1])
	return day
}

func FetchSettings(db *sqlx.DB, originalTable Table, table Table) (string, string, string, bool) {
	var field string
	var period string
	var cast string

	triggerName := originalTable.TriggerName()
	triggerComment := table.FetchTrigger(db, triggerName)

	comment := triggerComment
	if comment == "" {
		comment = table.FetchComment(db)
	}

	if comment != "" {
		parts := make([]string, 3)
		for i, part := range strings.Split(comment, ",") {
			parts[i] = strings.Split(part, ":")[1]
		}
		field = parts[0]
		period = parts[1]
		cast = parts[2]
	}

	declarative := triggerComment == ""

	return period, field, cast, declarative
}
