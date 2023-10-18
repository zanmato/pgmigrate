# pgmigrate

Applies migration files of the following format `[date][sequence]_[name].[up|down].sql` (`2006010200_init_schema.up.sql`).

Applied migrations will be stored in the table `__migrations`.

## Usage

```go
mg, err := migrate.NewMigrator(db, logger, basePath)
if err != nil {
  log.Fatalf("unable to create migrator: %s", err)
}

if err := mg.MigrateUp(context.Background()); err != nil {
  log.Fatalf("unable to create migrator: %s", err)
}
```