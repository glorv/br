package task

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/backup"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// BackupConfig is the configuration specific for backup tasks.
type BackupRawConfig struct {
	Config

	StartKey []byte
	EndKey []byte
	CF string
}

// DefineBackupRawFlags defines common flags for the backup command.
func DefineBackupRawFlags(flags *pflag.FlagSet) {
}

// ParseFromFlags parses the backup-related flags from the flag set.
func (cfg *BackupRawConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	start, err := flags.GetString("start")
	if err != nil {
		return err
	}
	cfg.StartKey, err = utils.ParseKey(flags, start)
	if err != nil {
		return err
	}
	end, err := flags.GetString("end")
	if err != nil {
		return err
	}
	cfg.EndKey, err = utils.ParseKey(flags, end)
	if err != nil {
		return err
	}

	if bytes.Compare(cfg.StartKey, cfg.EndKey) > 0 {
		return errors.New("input endKey must greater or equal than startKey")
	}

	cfg.CF, err = flags.GetString("cf")
	if err != nil {
		return err
	}
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunBackupRaw starts a backup task inside the current goroutine.
func RunBackupRaw(c context.Context, cmdName string, cfg *BackupRawConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return err
	}
	mgr, err := newMgr(ctx, cfg.PD)
	if err != nil {
		return err
	}
	defer mgr.Close()



	client, err := backup.NewBackupClient(ctx, mgr)
	if err != nil {
		return err
	}
	if err = client.SetStorage(ctx, u, cfg.SendCreds); err != nil {
		return err
	}

	backupTS, err := client.GetTS(ctx, 0)
	if err != nil {
		return err
	}

	defer summary.Summary(cmdName)

	backupRange := backup.Range{StartKey: cfg.StartKey, EndKey: cfg.EndKey}

	// The number of regions need to backup
	approximateRegions, err := mgr.GetRegionCount(ctx, backupRange.StartKey, backupRange.EndKey)

	summary.CollectInt("backup total regions", approximateRegions)

	// Backup
	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := utils.StartProgress(
		ctx, cmdName, int64(approximateRegions), !cfg.LogProgress)
	err = client.BackupRanges(
		ctx, []backup.Range{backupRange}, backupTS, nil, cfg, updateCh)
	if err != nil {
		return err
	}
	// Backup has finished
	close(updateCh)

	// Checksum
	err = client.SaveBackupMeta(ctx)
	if err != nil {
		return err
	}
	return nil
}