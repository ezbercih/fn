package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/fnproject/fn/api/common"
	"github.com/pressly/goose"
	"sort"
)

var (
	migrations = goose.Migrations{}
)

// each new migration will add corresponding column to the table def
var tables = [...]string{`CREATE TABLE IF NOT EXISTS routes (
	app_name varchar(256) NOT NULL,
	path varchar(256) NOT NULL,
	image varchar(256) NOT NULL,
	format varchar(16) NOT NULL,
	memory int NOT NULL,
	cpus int,
	timeout int NOT NULL,
	idle_timeout int NOT NULL,
	type varchar(16) NOT NULL,
	headers text NOT NULL,
	config text NOT NULL,
	created_at text,
	updated_at varchar(256),
	PRIMARY KEY (app_name, path)
);`,

	`CREATE TABLE IF NOT EXISTS apps (
	name varchar(256) NOT NULL PRIMARY KEY,
	config text NOT NULL,
	created_at varchar(256),
	updated_at varchar(256)
);`,

	`CREATE TABLE IF NOT EXISTS calls (
	created_at varchar(256) NOT NULL,
	started_at varchar(256) NOT NULL,
	completed_at varchar(256) NOT NULL,
	status varchar(256) NOT NULL,
	id varchar(256) NOT NULL,
	app_name varchar(256) NOT NULL,
	path varchar(256) NOT NULL,
	stats text,
	error text,
	PRIMARY KEY (id)
);`,

	`CREATE TABLE IF NOT EXISTS logs (
	id varchar(256) NOT NULL PRIMARY KEY,
	app_name varchar(256) NOT NULL,
	log text NOT NULL
);`,
}

func checkOldMigrationTableVersionIfExists(db *sql.DB) (version int64, dirty bool, err error) {
	migrationsTable := "schema_migrations"
	ctx := context.Background()

	q := db.QueryRowContext(
		ctx, "SELECT version, dirty FROM "+migrationsTable+" LIMIT 1")
	q.Scan(&version, &dirty)
	if err == sql.ErrNoRows {
		return -1, false, nil
	} else if err != nil {
		return -1, false, err
	}
	return version, dirty, nil
}

// copy of goose.sortAndConnetMigrations
func sortAndConnectMigrations(migrations goose.Migrations) goose.Migrations {
	sort.Sort(migrations)

	// now that we're sorted in the appropriate direction,
	// populate next and previous for each migration
	for i, m := range migrations {
		prev := int64(-1)
		if i > 0 {
			prev = migrations[i-1].Version
			migrations[i-1].Next = m.Version
		}
		migrations[i].Previous = prev
	}

	return migrations
}

func DownAll(driver string, db *sql.DB) error {
	goose.SetDialect(driver)
	migrations = sortAndConnectMigrations(migrations)

	for {
		currentVersion, err := goose.GetDBVersion(db)
		if err != nil {
			return err
		}

		current, err := migrations.Current(currentVersion)
		if err != nil {
			fmt.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if current.Version <= 1 {
			fmt.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if err = current.Down(db); err != nil {
			return err
		}
	}

}

func checkOldMigration(ctx context.Context, db *sql.DB) (int64, goose.Migrations, error) {
	log := common.Logger(ctx)
	migrationsSorted := sortAndConnectMigrations(migrations)
	current, dirty, err := checkOldMigrationTableVersionIfExists(db)
	if err != nil {
		return -1, nil, err
	}
	if dirty {
		log.Fatal("database corrupted")
	}
	log.Debug("old migration table version is: ", current)

	if current > 0 {
		// only partial upgrade, for the last version in old migration table
		return current, migrationsSorted[current:], nil
	}
	// full upgrade
	return -1, migrationsSorted, nil
}

func ApplyMigrations(ctx context.Context, driver string, db *sql.DB) error {
	goose.SetDialect(driver)
	log := common.Logger(ctx)

	for _, v := range tables {
		_, err := db.ExecContext(ctx, v)
		if err != nil {
			return err
		}
	}

	// current can equal to -1, 0 or current version
	// which is suppose to be greater than zero
	gooseCurrent, err := goose.GetDBVersion(db)
	log.Debug("goose: current datastore version: ", gooseCurrent)
	if err != nil {
		if err != goose.ErrNoNextVersion {
			return err
		}
	}

	// will run full or partial upgrades by skipping already
	// applied migration at the old database

	migrateCurrent, left, err := checkOldMigration(ctx, db)
	if err != nil {
		return err
	}

	// do not run the migrations if goose version is higher than old migrate version
	if gooseCurrent < migrateCurrent {
		log.Debug("migrations to apply: ", len(left))
		for _, m := range left {
			if err := m.Up(db); err != nil {
				log.Error("migrations upgrade error: ", err.Error())
				return err
			}
		}
		log.Debug("goose: next datastore will be: ", migrateCurrent+1)
	}
	log.Debug("goose: next datastore will be: ", gooseCurrent+1)

	return nil
}
