package stringstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cbehopkins/bobbob"
)

type Config struct {
	FilePath           string
	MaxNumberOfStrings int
	StartingObjectId   bobbob.ObjectId
	ObjectIdInterval   bobbob.ObjectId
	WriteFlushInterval time.Duration
	WriteMaxBatchBytes int
	DiskWritePoolSize  int // Number of concurrent disk writer workers (default: 4)
	UnloadOnFull       bool
	UnloadAfterIdle    time.Duration
	UnloadScanInterval time.Duration
	LazyLoadShards     bool
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.MaxNumberOfStrings <= 0 {
		return Config{}, errors.New("MaxNumberOfStrings must be > 0")
	}
	if cfg.StartingObjectId <= 0 {
		return Config{}, errors.New("StartingObjectId must be > 0")
	}
	if cfg.ObjectIdInterval <= 0 {
		return Config{}, errors.New("ObjectIdInterval must be > 0")
	}
	if cfg.WriteFlushInterval <= 0 {
		cfg.WriteFlushInterval = defaultFlushInterval
	}
	if cfg.WriteMaxBatchBytes <= 0 {
		cfg.WriteMaxBatchBytes = defaultMaxBatchBytes
	}
	if cfg.DiskWritePoolSize <= 0 {
		cfg.DiskWritePoolSize = 1 // Default to 1 worker (sequential processing, but offloads from writeWorker)
	}
	if cfg.UnloadAfterIdle < 0 {
		return Config{}, errors.New("UnloadAfterIdle must be >= 0")
	}
	if cfg.UnloadScanInterval < 0 {
		return Config{}, errors.New("UnloadScanInterval must be >= 0")
	}
	if cfg.UnloadAfterIdle > 0 && cfg.UnloadScanInterval <= 0 {
		cfg.UnloadScanInterval = defaultUnloadScanInterval(cfg.UnloadAfterIdle)
	}

	path := cfg.FilePath
	if path == "" {
		tmpDir := os.TempDir()
		path = filepath.Join(tmpDir, fmt.Sprintf("bobbob-stringstore-%d.blob", time.Now().UnixNano()))
	}
	cfg.FilePath = path

	return cfg, nil
}

func defaultUnloadScanInterval(idle time.Duration) time.Duration {
	if idle <= 0 {
		return 0
	}
	interval := idle / 2
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

func shardFilePath(basePath string, shardIndex int) string {
	dir := filepath.Dir(basePath)
	base := filepath.Base(basePath)
	ext := filepath.Ext(base)
	base = strings.TrimSuffix(base, ext)
	if ext == "" {
		ext = ".blob"
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%04d%s", base, shardIndex, ext))
}
