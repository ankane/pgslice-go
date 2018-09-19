package pgslice

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
	"time"
)

type Table struct {
	Schema string
	Name   string
}

type Sequence struct {
	Name   string
	Column string
}

func (t Table) IntermediateTable() Table {
	return Table{Schema: t.Schema, Name: t.Name + "_intermediate"}
}

func (t Table) RetiredTable() Table {
	return Table{Schema: t.Schema, Name: t.Name + "_retired"}
}

func (t Table) TriggerName() string {
	return t.Name + "_insert_trigger"
}

func (t Table) FullName() string {
	return strings.Join([]string{t.Schema, t.Name}, ".")
}

func (t Table) Exists(db *sqlx.DB) bool {
	return len(t.ExistingTables(db, t.Name)) > 0
}

func (t Table) Sequences(db *sqlx.DB) []Sequence {
	query := `
SELECT
  s.relname as name,
  a.attname as column
FROM pg_class s
  JOIN pg_depend d ON d.objid = s.oid
  JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid
  JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)
  JOIN pg_namespace n ON n.oid = s.relnamespace
WHERE s.relkind = 'S'
  AND n.nspname = $1
  AND t.relname = $2
  `
	sequences := []Sequence{}
	err := db.Select(&sequences, query, t.Schema, t.Name)
	if err != nil {
		log.Fatal(err)
	}
	return sequences
}

func (t Table) ExistingTables(db *sqlx.DB, like string) []Table {
	query := "SELECT schemaname AS schema, tablename as name FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename LIKE $2 ORDER BY 1, 2"

	tables := []Table{}
	err := db.Select(&tables, query, t.Schema, like)
	if err != nil {
		log.Fatal(err)
	}
	return tables
}

func (t Table) Partitions(db *sqlx.DB) []Table {
	query := `
SELECT
  nmsp_child.nspname  AS schema,
  child.relname       AS name
FROM pg_inherits
  JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
  JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
  JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
  JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE
  nmsp_parent.nspname = $1 AND
  parent.relname = $2
  `

	tables := []Table{}
	err := db.Select(&tables, query, t.Schema, t.Name)
	if err != nil {
		log.Fatal(err)
	}
	return tables
}

func (t Table) Columns(db *sqlx.DB) []string {
	query := "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"

	keys := []string{}
	err := db.Select(&keys, query, t.Schema, t.Name)
	if err != nil {
		log.Fatal(err)
	}
	return keys
}

func (t Table) ForeignKeys(db *sqlx.DB) []string {
	query := fmt.Sprintf("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = %s AND contype ='f'", t.Regclass())

	keys := []string{}
	err := db.Select(&keys, query)
	if err != nil {
		log.Fatal(err)
	}
	return keys
}

func (t Table) MaxID(db *sqlx.DB, primaryKey string, where string, below int) int {
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", QuoteIdent(primaryKey), QuoteTable(t))

	conditions := []string{}
	if below != -1 {
		conditions = append(conditions, fmt.Sprintf("%s <= %d", QuoteIdent(primaryKey), below))
	}
	if where != "" {
		conditions = append(conditions, where)
	}
	if len(conditions) > 0 {
		query = query + " WHERE " + strings.Join(conditions, " AND ")
	}

	var max int
	err := db.Get(&max, query)
	if err != nil {
		return 0
	}
	return max
}

func (t Table) MinID(db *sqlx.DB, primaryKey string, column string, cast string, startingTime time.Time, where string) int {
	query := fmt.Sprintf("SELECT MIN(%s) FROM %s", QuoteIdent(primaryKey), QuoteTable(t))

	conditions := []string{}
	if startingTime != (time.Time{}) {
		conditions = append(conditions, fmt.Sprintf("%s >= %s", QuoteIdent(column), SQLDate(startingTime, cast, true)))
	}
	if where != "" {
		conditions = append(conditions, where)
	}
	if len(conditions) > 0 {
		query = query + " WHERE " + strings.Join(conditions, " AND ")
	}

	var min int
	err := db.Get(&min, query)
	if err != nil {
		return 1
	}
	return min
}

func (t Table) ColumnCast(db *sqlx.DB, column string) string {
	var dataType string
	err := db.Get(&dataType, "SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3", t.Schema, t.Name, column)
	if err != nil {
		log.Fatal(err)
	}
	if dataType == "timestamp with time zone" {
		return "timestamptz"
	}
	return "date"
}

func (t Table) PrimaryKey(db *sqlx.DB) []string {
	query := `
    SELECT
      pg_attribute.attname
    FROM
      pg_index, pg_class, pg_attribute, pg_namespace
    WHERE
      nspname = $1 AND
      relname = $2 AND
      indrelid = pg_class.oid AND
      pg_class.relnamespace = pg_namespace.oid AND
      pg_attribute.attrelid = pg_class.oid AND
      pg_attribute.attnum = any(pg_index.indkey) AND
      indisprimary
  `

	keys := []string{}
	err := db.Select(&keys, query, t.Schema, t.Name)
	if err != nil {
		log.Fatal(err)
	}
	return keys
}

func (t Table) IndexDefs(db *sqlx.DB) []string {
	defs := []string{}
	err := db.Select(&defs, fmt.Sprintf("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = %s AND indisprimary = 'f'", t.Regclass()))
	if err != nil {
		log.Fatal(err)
	}
	return defs
}

func (t Table) FetchComment(db *sqlx.DB) string {
	var comment string
	err := db.Get(&comment, fmt.Sprintf("SELECT COALESCE(obj_description(%s), '') AS comment", t.Regclass()))
	if err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		log.Fatal(err)
	}
	return comment
}

func (t Table) FetchTrigger(db *sqlx.DB, triggerName string) string {
	var trigger string
	err := db.Get(&trigger, fmt.Sprintf("SELECT obj_description(oid, 'pg_trigger') AS comment FROM pg_trigger WHERE tgname = $1 AND tgrelid = %s", t.Regclass()), triggerName)
	if err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		log.Fatal(err)
	}
	return trigger
}

func (t Table) Regclass() string {
	return fmt.Sprintf("'%s'::regclass", QuoteTable(t))
}
