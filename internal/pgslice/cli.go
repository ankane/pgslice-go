package pgslice

import (
	"log"
	"os"

	"github.com/urfave/cli"
)

func RunApp(args []string) {
	app := cli.NewApp()
	app.Usage = "Postgres partitioning as easy as pie"
	app.UsageText = "pgslice COMMAND [options]"
	app.Version = "0.1.0"

	app.Commands = []cli.Command{
		{
			Name:  "prep",
			Usage: "Create an intermediate table for partitioning",
			Action: func(ctx *cli.Context) error {
				return Prep(ctx)
			},
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "no-partition",
					Usage: "Don't partition the table",
				},
				cli.BoolFlag{
					Name:  "trigger-based",
					Usage: "Use trigger-based partitioning",
				},
			},
		},
		{
			Name:  "add_partitions",
			Usage: "Add partitions",
			Action: func(ctx *cli.Context) error {
				return AddPartitions(ctx)
			},
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "intermediate",
					Usage: "Add to intermediate table",
				},
				cli.IntFlag{
					Name:  "past",
					Usage: "Number of past partitions to add",
					Value: 0,
				},
				cli.IntFlag{
					Name:  "future",
					Usage: "Number of future partitions to add",
					Value: 0,
				},
			},
		},
		{
			Name:  "fill",
			Usage: "Fill the partitions in batches",
			Action: func(ctx *cli.Context) error {
				return Fill(ctx)
			},
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "batch-size",
					Usage: "Batch size",
					Value: 10000,
				},
				cli.BoolFlag{
					Name:  "swapped",
					Usage: "Use swapped table",
				},
				cli.StringFlag{
					Name:  "source-table",
					Usage: "Source table",
				},
				cli.StringFlag{
					Name:  "dest-table",
					Usage: "Destination table",
				},
				cli.IntFlag{
					Name:  "start",
					Usage: "Primary key to start",
				},
				cli.StringFlag{
					Name:  "where",
					Usage: "Conditions to filter",
				},
				cli.IntFlag{
					Name:  "sleep",
					Usage: "Seconds to sleep between batches",
				},
			},
		},
		{
			Name:  "analyze",
			Usage: "Analyze tables",
			Action: func(ctx *cli.Context) error {
				return Analyze(ctx)
			},
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "swapped",
					Usage: "Use swapped table",
				},
			},
		},
		{
			Name:  "swap",
			Usage: "Swap the intermediate table with the original table",
			Action: func(ctx *cli.Context) error {
				return Swap(ctx)
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "lock-timeout",
					Value: "5s",
					Usage: "Lock timeout",
				},
			},
		},
		{
			Name:  "unprep",
			Usage: "Undo prep",
			Action: func(ctx *cli.Context) error {
				return Unprep(ctx)
			},
		},
		{
			Name:  "unswap",
			Usage: "Undo swap",
			Action: func(ctx *cli.Context) error {
				return Unswap(ctx)
			},
		},
	}

	sharedFlags := []cli.Flag{
		cli.StringFlag{
			Name:  "url",
			Usage: "Database URL",
		},
		cli.BoolFlag{
			Name:  "dry-run",
			Usage: "Print statements without executing",
		},
	}

	for i, command := range app.Commands {
		app.Commands[i].Flags = append(command.Flags, sharedFlags...)
	}

	err := app.Run(args)
	if err != nil {
		log.Fatal(err)
	}
}

func Execute() {
	RunApp(os.Args)
}
