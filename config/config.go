package config

import (
	"errors"
	"path/filepath"
)

/*"fmt"
"os"
"path/filepath"
"time"

"github.com/pkg/errors"*/

//-----------------------------------------------------------------------------
// EventpoolConfig

// EventpoolConfig defines the configuration options for the Tendermint mempool
type EventpoolConfig struct {
	RootDir        string `mapstructure:"home"`
	Recheck        bool   `mapstructure:"recheck"`
	Broadcast      bool   `mapstructure:"broadcast"`
	WalPath        string `mapstructure:"wal_dir"`
	Size           int    `mapstructure:"size"`
	MaxEventsBytes int64  `mapstructure:"max_events_bytes"`
	CacheSize      int    `mapstructure:"cache_size"`
}

// DefaultEventpoolConfig returns a default configuration for the Tendermint mempool
func DefaultEventpoolConfig() *EventpoolConfig {
	return &EventpoolConfig{
		Recheck:   true,
		Broadcast: true,
		WalPath:   "",
		// Each signature verification takes .5ms, Size reduced until we implement
		// ABCI Recheck
		Size:           5000,
		MaxEventsBytes: 1024 * 1024 * 1024, // 1GB
		CacheSize:      10000,
	}
}

// TestEventpoolConfig returns a configuration for testing the Tendermint mempool
func TestEventpoolConfig() *EventpoolConfig {
	cfg := DefaultEventpoolConfig()
	cfg.CacheSize = 1000
	return cfg
}

// WalDir returns the full path to the mempool's write-ahead log
func (cfg *EventpoolConfig) WalDir() string {
	return rootify(cfg.WalPath, cfg.RootDir)
}

// WalEnabled returns true if the WAL is enabled.
func (cfg *EventpoolConfig) WalEnabled() bool {
	return cfg.WalPath != ""
}

// ValidateBasic performs basic validation (checking param bounds, etc.) and
// returns an error if any check fails.
func (cfg *EventpoolConfig) ValidateBasic() error {
	if cfg.Size < 0 {
		return errors.New("size can't be negative")
	}
	if cfg.MaxEventsBytes < 0 {
		return errors.New("max_txs_bytes can't be negative")
	}
	if cfg.CacheSize < 0 {
		return errors.New("cache_size can't be negative")
	}
	return nil
}

// helper function to make config creation independent of root dir
func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}
