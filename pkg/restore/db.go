package restore

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

// DB is a TiDB instance, not thread-safe.
type DB struct {
	se session.Session
}

// NewDB returns a new DB
func NewDB(store kv.Storage) (*DB, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Set SQL mode to None for avoiding SQL compatibility problem
	_, err = se.Execute(context.Background(), "set @@sql_mode=''")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DB{
		se: se,
	}, nil
}

// ExecDDL executes the query of a ddl job.
func (db *DB) ExecDDL(ctx context.Context, ddlJob *model.Job) error {
	var err error
	if ddlJob.BinlogInfo.TableInfo != nil {
		switchDbSQL := fmt.Sprintf("use %s;", ddlJob.SchemaName)
		_, err = db.se.Execute(ctx, switchDbSQL)
		if err != nil {
			log.Error("switch db failed",
				zap.String("query", switchDbSQL),
				zap.String("db", ddlJob.SchemaName),
				zap.Error(err))
			return errors.Trace(err)
		}
	}
	_, err = db.se.Execute(ctx, ddlJob.Query)
	if err != nil {
		log.Error("execute ddl query failed",
			zap.String("query", ddlJob.Query),
			zap.String("db", ddlJob.SchemaName),
			zap.Int64("historySchemaVersion", ddlJob.BinlogInfo.SchemaVersion),
			zap.Error(err))
	}
	return errors.Trace(err)
}

// CreateDatabase executes a CREATE DATABASE SQL.
func (db *DB) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	var buf bytes.Buffer
	err := executor.ConstructResultOfShowCreateDatabase(db.se, schema, true, &buf)
	if err != nil {
		log.Error("build create database SQL failed", zap.Stringer("db", schema.Name), zap.Error(err))
		return errors.Trace(err)
	}
	createSQL := buf.String()
	_, err = db.se.Execute(ctx, createSQL)
	if err != nil {
		log.Error("create database failed", zap.String("query", createSQL), zap.Error(err))
	}
	return errors.Trace(err)
}

// CreateTable executes a CREATE TABLE SQL.
func (db *DB) CreateTable(ctx context.Context, table *utils.Table) error {
	var buf bytes.Buffer
	schema := table.Info
	err := executor.ConstructResultOfShowCreateTable(db.se, schema, newIDAllocator(schema.AutoIncID), &buf)
	if err != nil {
		log.Error(
			"build create table SQL failed",
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", schema.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	switchDbSQL := fmt.Sprintf("use %s;", table.Db.Name)
	_, err = db.se.Execute(ctx, switchDbSQL)
	if err != nil {
		log.Error("switch db failed",
			zap.String("SQL", switchDbSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	createSQL := buf.String()
	// Insert `IF NOT EXISTS` statement to skip the created tables
	words := strings.SplitN(createSQL, " ", 3)
	if len(words) > 2 && strings.ToUpper(words[0]) == "CREATE" && strings.ToUpper(words[1]) == "TABLE" {
		createSQL = "CREATE TABLE IF NOT EXISTS " + words[2]
	}
	_, err = db.se.Execute(ctx, createSQL)
	if err != nil {
		log.Error("create table failed",
			zap.String("SQL", createSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", table.Info.Name),
			zap.Error(err))
		return errors.Trace(err)
	}
	alterAutoIncIDSQL := fmt.Sprintf(
		"alter table %s auto_increment = %d",
		escapeTableName(schema.Name),
		schema.AutoIncID)
	_, err = db.se.Execute(ctx, alterAutoIncIDSQL)
	if err != nil {
		log.Error("alter AutoIncID failed",
			zap.String("query", alterAutoIncIDSQL),
			zap.Stringer("db", table.Db.Name),
			zap.Stringer("table", table.Info.Name),
			zap.Error(err))
	}
	return errors.Trace(err)
}

// Close closes the connection
func (db *DB) Close() {
	db.se.Close()
}

// FilterDDLJobs filters ddl jobs
func FilterDDLJobs(allDDLJobs []*model.Job, tables []*utils.Table) (ddlJobs []*model.Job) {
	// Sort the ddl jobs by schema version in descending order.
	sort.Slice(allDDLJobs, func(i, j int) bool {
		return allDDLJobs[i].BinlogInfo.SchemaVersion > allDDLJobs[j].BinlogInfo.SchemaVersion
	})
	dbs := getDatabases(tables)
	for _, db := range dbs {
		// These maps is for solving some corner case.
		// e.g. let "t=2" indicates that the id of database "t" is 2, if the ddl execution sequence is:
		// rename "a" to "b"(a=1) -> drop "b"(b=1) -> create "b"(b=2) -> rename "b" to "a"(a=2)
		// Which we cannot find the "create" DDL by name and id directly.
		// To cover †his case, we must find all names and ids the database/table ever had.
		dbIDs := make(map[int64]bool)
		dbIDs[db.ID] = true
		dbNames := make(map[string]bool)
		dbNames[db.Name.String()] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.DBInfo != nil {
				if dbIDs[job.SchemaID] || dbNames[job.BinlogInfo.DBInfo.Name.String()] {
					ddlJobs = append(ddlJobs, job)
					// The the jobs executed with the old id, like the step 2 in the example above.
					dbIDs[job.SchemaID] = true
					// For the jobs executed after rename, like the step 3 in the example above.
					dbNames[job.BinlogInfo.DBInfo.Name.String()] = true
				}
			}
		}
	}

	for _, table := range tables {
		tableIDs := make(map[int64]bool)
		tableIDs[table.Info.ID] = true
		tableNames := make(map[string]bool)
		tableNames[table.Info.Name.String()] = true
		for _, job := range allDDLJobs {
			if job.BinlogInfo.TableInfo != nil {
				if tableIDs[job.TableID] || tableNames[job.BinlogInfo.TableInfo.Name.String()] {
					ddlJobs = append(ddlJobs, job)
					tableIDs[job.TableID] = true
					// For truncate table, the id may be changed
					tableIDs[job.BinlogInfo.TableInfo.ID] = true
					tableNames[job.BinlogInfo.TableInfo.Name.String()] = true
				}
			}
		}
	}
	return ddlJobs
}

func getDatabases(tables []*utils.Table) (dbs []*model.DBInfo) {
	dbIDs := make(map[int64]bool)
	for _, table := range tables {
		if !dbIDs[table.Db.ID] {
			dbs = append(dbs, table.Db)
			dbIDs[table.Db.ID] = true
		}
	}
	return
}
