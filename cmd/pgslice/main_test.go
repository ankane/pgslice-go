package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	db, err := sql.Open("postgres", "postgres://localhost/pgslice_test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
  SET client_min_messages = warning;
  DROP TABLE IF EXISTS "Posts_intermediate" CASCADE;
  DROP TABLE IF EXISTS "Posts" CASCADE;
  DROP TABLE IF EXISTS "Posts_retired" CASCADE;
  DROP FUNCTION IF EXISTS "Posts_insert_trigger"();
  DROP TABLE IF EXISTS "Users" CASCADE;
  CREATE TABLE "Users" (
    "Id" SERIAL PRIMARY KEY
  );
  CREATE TABLE "Posts" (
    "Id" SERIAL PRIMARY KEY,
    "UserId" INTEGER,
    "createdAt" timestamp,
    CONSTRAINT "foreign_key_1" FOREIGN KEY ("UserId") REFERENCES "Users"("Id")
  );
  CREATE INDEX ON "Posts" ("createdAt");
  INSERT INTO "Posts" ("createdAt") SELECT NOW() FROM generate_series(1, 10000) n;
  `)
	if err != nil {
		log.Fatal(err)
	}

	retCode := m.Run()
	os.Exit(retCode)
}

func TestDay(t *testing.T) {
	AssertPeriod(t, "day", false)
}

func TestMonth(t *testing.T) {
	AssertPeriod(t, "month", false)
}

func TestYear(t *testing.T) {
	AssertPeriod(t, "year", false)
}

func TestNoPartition(t *testing.T) {
	RunCommand("prep Posts --no-partition")
	RunCommand("fill Posts")
	RunCommand("swap Posts")
	RunCommand("unswap Posts")
	RunCommand("unprep Posts")
}

func TestTriggerBased(t *testing.T) {
	AssertPeriod(t, "day", true)
}

func AssertPeriod(t *testing.T, period string, triggerBased bool) {
	triggerStr := ""
	if triggerBased {
		triggerStr = " --trigger-based"
	}
	RunCommand(fmt.Sprintf("prep Posts createdAt %s%s", period, triggerStr))
	RunCommand("add_partitions Posts --intermediate --past 1 --future 1")
	RunCommand("fill Posts")
	RunCommand("analyze Posts")
	RunCommand("swap Posts")
	RunCommand("fill Posts --swapped")
	RunCommand("add_partitions Posts --future 3")
	RunCommand("unswap Posts")
	RunCommand("unprep Posts")
}

func RunCommand(command string) {
	fmt.Println(fmt.Sprintf("pgslice %s", command))
	fmt.Println("")
	RunApp(strings.Split(fmt.Sprintf("pgslice %s --url %s", command, "postgres://localhost/pgslice_test?sslmode=disable"), " "))
	fmt.Println("")
}
