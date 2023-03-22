package db

import (
	"context"

	"github.com/flanksource/commons/logger"
	"github.com/flanksource/duty"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
	"gorm.io/gorm"
)

// connection variables
var (
	ConnectionString string
	Schema           = "public"
	LogLevel         = "info"
	runMigrations    = false
)

func Flags(flags *pflag.FlagSet) {
	flags.StringVar(&ConnectionString, "db", "DB_URL", "Connection string for the postgres database")
	flags.StringVar(&Schema, "db-schema", "public", "")
	flags.StringVar(&LogLevel, "db-log-level", "warn", "")
	flags.BoolVar(&runMigrations, "db-migrations", false, "Run database migrations")
}

var Pool *pgxpool.Pool
var gormDB *gorm.DB

// MustInit initializes the database or fatally exits
func MustInit() {
	if err := Init(ConnectionString); err != nil {
		logger.Fatalf("Failed to initialize db: %v", err.Error())
	}
}

func Init(connection string) error {
	var err error
	Pool, err = duty.NewPgxPool(connection)
	if err != nil {
		return err
	}

	conn, err := Pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()

	if err := conn.Ping(context.Background()); err != nil {
		return err
	}

	gormDB, err = duty.NewGorm(connection, duty.DefaultGormConfig())
	if err != nil {
		return err
	}

	if runMigrations {
		if err = duty.Migrate(connection); err != nil {
			return err
		}
	}

	return nil
}
