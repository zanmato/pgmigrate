package test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	migrate "github.com/zanmato/pgmigrate"
)

type logger struct {
	t *testing.T
}

func (l *logger) Infof(template string, args ...interface{}) {
	l.t.Logf("[INFO] "+template+"\n", args)
}

func (l *logger) Warnf(template string, args ...interface{}) {
	l.t.Logf("[WARN] "+template+"\n", args)
}

var db *sql.DB

func TestMain(m *testing.M) {
	var dbErr error
	db, dbErr = sql.Open("pgx", "postgres://pgmigrate:pgmigrate@localhost:5452/pgmigrate?sslmode=disable")
	if dbErr != nil {
		log.Fatalf("unable to open database: %s", dbErr)
	}

	exitCode := m.Run()
	db.Close()

	os.Exit(exitCode)
}

func TestMigrateUp(t *testing.T) {
	l := &logger{t: t}

	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS __migrations, test_table_1, test_table_2")
	})

	mg, err := migrate.NewMigrator(db, l, "./testdata/migrations")
	if err != nil {
		t.Fatalf("unable to create migrator: %s", err)
	}

	if err := mg.MigrateUp(context.Background()); err != nil {
		t.Fatalf("unable to create migrator: %s", err)
	}

	// Assert that the tables were created and the migrations table was updated
	tableExists := []string{
		"__migrations",
		"test_table_1",
		"test_table_2",
	}
	for _, tbl := range tableExists {
		var exists bool
		db.QueryRow(
			`SELECT EXISTS (
			SELECT *
			FROM pg_tables
			WHERE 
					schemaname = 'public' AND 
					tablename  = $1
			);`,
			tbl,
		).Scan(&exists)
		if !exists {
			t.Errorf("expected table %s to exist", tbl)
		}
	}

	migrationsExists := []struct {
		version int
		name    string
	}{
		{2023100100, "test"},
		{2023100101, "test2"},
	}
	for _, mgr := range migrationsExists {
		var exists bool
		db.QueryRow(
			`SELECT EXISTS (
			SELECT *
			FROM __migrations
			WHERE 
				version = $1 AND
				name = $2
			);`,
			mgr.version,
			mgr.name,
		).Scan(&exists)
		if !exists {
			t.Errorf("expected migration %d_%s to exist", mgr.version, mgr.name)
		}
	}
}

func TestMigrateDown(t *testing.T) {
	l := &logger{t: t}

	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS __migrations, test_table_1, test_table_2")
	})

	mg, err := migrate.NewMigrator(db, l, "./testdata/migrations")
	if err != nil {
		t.Fatalf("unable to create migrator: %s", err)
	}

	if err := mg.MigrateUp(context.Background()); err != nil {
		t.Fatalf("unable to create migrator: %s", err)
	}

	if err := mg.MigrateDown(context.Background(), 2023100100); err != nil {
		t.Fatalf("unable to create migrator: %s", err)
	}

	// Assert that the tables were created and the migrations table was updated
	tableExists := []string{
		"__migrations",
		"test_table_1",
	}
	for _, tbl := range tableExists {
		var exists bool
		db.QueryRow(
			`SELECT EXISTS (
			SELECT *
			FROM pg_tables
			WHERE 
					schemaname = 'public' AND 
					tablename  = $1
			);`,
			tbl,
		).Scan(&exists)
		if !exists {
			t.Errorf("expected table %s to exist", tbl)
		}
	}

	migrationsExists := []struct {
		version int
		name    string
	}{
		{2023100100, "test"},
	}
	for _, mgr := range migrationsExists {
		var exists bool
		db.QueryRow(
			`SELECT EXISTS (
			SELECT *
			FROM __migrations
			WHERE 
				version = $1 AND
				name = $2
			);`,
			mgr.version,
			mgr.name,
		).Scan(&exists)
		if !exists {
			t.Errorf("expected migration %d_%s to exist", mgr.version, mgr.name)
		}
	}
}
