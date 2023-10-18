package pgmigrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var ErrNoMigrations = fmt.Errorf("no migrations found")

type Logger interface {
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
}

type migrator struct {
	db        *sql.DB
	basePath  string
	fileRegex *regexp.Regexp
	logger    Logger
}

type migrationFile struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
}

// NewMigrator creates a new migrator instance.
func NewMigrator(db *sql.DB, logger Logger, basePath string) (*migrator, error) {
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS __migrations (
			version int PRIMARY KEY,
			name TEXT NOT NULL
		)`,
	); err != nil {
		return nil, err
	}

	return &migrator{
		db:        db,
		basePath:  basePath,
		fileRegex: regexp.MustCompile(`^(\d{10})_(.*)\.(up|down)\.sql$`),
		logger:    logger,
	}, nil
}

// MigrateDown will rollback all migrations that were applied after the specified version.
func (m *migrator) MigrateDown(ctx context.Context, version int) error {
	// Find which migrations were applied after the specified one
	var res []byte
	if err := m.db.QueryRowContext(
		ctx,
		`SELECT json_agg(
			sm
			ORDER BY sm.version DESC
		)
		FROM __migrations sm
		WHERE sm.version > $1`,
		version,
	).Scan(&res); err != nil {
		return err
	}

	if len(res) == 0 {
		return nil
	}

	var rollbackMigrations []migrationFile
	if err := json.Unmarshal(res, &rollbackMigrations); err != nil {
		return err
	}

	if len(rollbackMigrations) == 0 {
		return fmt.Errorf("no migrations to rollback")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, mg := range rollbackMigrations {
		downFilepath := filepath.Join(m.basePath,
			fmt.Sprintf("%d_%s.down.sql", mg.Version, mg.Name),
		)

		if _, err := os.Stat(downFilepath); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			if os.IsNotExist(err) {
				return fmt.Errorf("could not find down file for version %s", mg)
			}

			return err
		}

		mgSource, err := os.ReadFile(downFilepath)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}

		m.logger.Infof("rolling back migration %s", mg)
		if _, err := tx.ExecContext(ctx, string(mgSource)); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}

		if _, err := tx.ExecContext(ctx, "DELETE FROM __migrations WHERE version = $1", mg.Version); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}
	}

	return tx.Commit()
}

// MigrateUp will apply all available migrations that have not been applied yet.
func (m *migrator) MigrateUp(ctx context.Context) error {
	files, err := os.ReadDir(m.basePath)
	if err != nil {
		return err
	}

	var (
		availableMigrations []migrationFile
		matches             []string
	)

	// Find all available migrations (up files)
	for _, f := range files {
		if f.IsDir() || strings.HasPrefix(f.Name(), ".") {
			continue
		}

		matches = m.fileRegex.FindStringSubmatch(f.Name())
		if len(matches) < 4 {
			m.logger.Warnf("file %s is not formatted correctly", f.Name())
			continue
		}

		if matches[3] != "up" {
			continue
		}

		v, err := strconv.Atoi(matches[1])
		if err != nil {
			return fmt.Errorf("failed extracting version for %s: %w", f.Name(), err)
		}

		availableMigrations = append(availableMigrations, migrationFile{
			Version: v,
			Name:    matches[2],
		})
	}

	if len(availableMigrations) == 0 {
		return ErrNoMigrations
	}

	// Find unapplied and diff migrations
	inp, err := json.Marshal(availableMigrations)
	if err != nil {
		return err
	}

	var (
		unappliedRes []byte
		diffRes      []byte
	)
	if err := m.db.QueryRowContext(
		ctx,
		`SELECT 
			(
				SELECT
				json_agg(x)
				FROM json_to_recordset($1) x(version int, name text)
				WHERE NOT EXISTS (
					SELECT * FROM __migrations sm WHERE sm.version = x.version
				)
			) AS unappliedm,
			(
				SELECT
				json_agg(sm)
				FROM __migrations sm
				LEFT JOIN json_to_recordset($1) x(version int, name text) ON x.version = sm.version
				WHERE x.version IS NULL
			) AS diffm
		`,
		inp,
	).Scan(&unappliedRes, &diffRes); err != nil {
		return err
	}

	// Find migrations that exist in the database but not on disk
	if len(diffRes) > 0 {
		var diffMigrations []migrationFile
		if err := json.Unmarshal(diffRes, &diffMigrations); err != nil {
			return err
		}

		if len(diffMigrations) > 0 {
			m.logger.Warnf(
				"found %d applied migration(s) that do not exist on disk: %v",
				len(diffMigrations),
				diffMigrations,
			)
		}
	}

	if len(unappliedRes) == 0 {
		return nil
	}

	var unappliedMigrations []migrationFile
	if err := json.Unmarshal(unappliedRes, &unappliedMigrations); err != nil {
		return err
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Apply each one of the unapplied migrations
	var migrationFilename string
	for _, mg := range unappliedMigrations {
		migrationFilename = fmt.Sprintf("%d_%s.up.sql", mg.Version, mg.Name)

		// Read migration source from disk
		mgSource, err := os.ReadFile(filepath.Join(m.basePath, migrationFilename))
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}

		m.logger.Infof("applying migration %s", mg)

		// Apply migration source
		if _, err := tx.ExecContext(
			ctx,
			string(mgSource),
		); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}

		// Add migration to __migrations table
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO __migrations (version, name) VALUES ($1, $2)",
			mg.Version,
			mg.Name,
		); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback migration transaction: %w", rollbackErr)
			}

			return err
		}
	}

	return tx.Commit()
}

func (m migrationFile) String() string {
	return fmt.Sprintf("%d_%s", m.Version, m.Name)
}
