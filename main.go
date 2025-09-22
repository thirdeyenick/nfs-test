package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"database/sql"

	"github.com/Cyberax/go-nfs-client/nfs4"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/rand"
)

type FileInfo struct {
	Name  string    `json:"name"`
	IsDir bool      `json:"is_dir"`
	Size  uint64    `json:"size"`
	Mtime time.Time `json:"mtime"`
}

var (
	server                 = os.Getenv("NFS_SERVER") // e.g. "192.168.1.50"
	share                  = os.Getenv("NFS_SHARE")  // e.g. "/export/data"
	listenPort             = os.Getenv("PORT")
	timeoutEnv             = os.Getenv("TIMEOUT")
	machineName            = os.Getenv("MACHINE_NAME")
	uidEnv                 = os.Getenv("UID")
	gidEnv                 = os.Getenv("GID")
	storagePathEnv         = os.Getenv("STORAGE_PATH")
	sqliteStoragePathEnv   = os.Getenv("SQLITE_STORAGE_PATH")
	podNameEnv             = os.Getenv("POD_NAME")
	sqliteWriteIntervalEnv = os.Getenv("WRITE_INTERVAL")
	sqliteReadIntervalEnv  = os.Getenv("READ_INTERVAL")
)

func main() {
	if server == "" || share == "" {
		log.Fatal("Must set NFS_SERVER and NFS_SHARE environment variables")
	}
	if listenPort == "" {
		listenPort = ":8080"
	}
	if timeoutEnv == "" {
		timeoutEnv = "20s"
	}
	if machineName == "" {
		machineName = "its-me"
	}
	if sqliteWriteIntervalEnv == "" {
		sqliteWriteIntervalEnv = "1s"
	}
	if sqliteReadIntervalEnv == "" {
		sqliteReadIntervalEnv = "10s"
	}

	timeout, err := time.ParseDuration(timeoutEnv)
	if err != nil {
		log.Fatalf("timeout can not be parsed: %v", err)
	}

	uid, err := strconv.Atoi(uidEnv)
	if err != nil {
		log.Fatalf("can not parse UID env variable: %v", err)
	}
	gid, err := strconv.Atoi(gidEnv)
	if err != nil {
		log.Fatalf("can not parse GID env variable: %v", err)
	}

	logger := slog.Default().With("pod_name", podNameEnv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle termination signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		logger.Info("received shutdown signal", "podName", podNameEnv)
		cancel()
	}()

	// setup sqlite routines
	if strings.TrimSpace(sqliteStoragePathEnv) != "" {
		closeDB, err := setupSqlite(ctx, sqliteStoragePathEnv, podNameEnv, logger, sqliteReadIntervalEnv, sqliteWriteIntervalEnv)
		if err != nil {
			log.Fatalf("error when setting up sqlite tests: %v", err)
			return
		}
		defer func() {
			if err := closeDB(); err != nil {
				logger.Error("can not close DB", slog.Any("error", err))
			}
		}()
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/write-storage", func(w http.ResponseWriter, r *http.Request) {
		response := &response{writer: w, logger: logger}
		defer response.handle()

		if storagePathEnv == "" {
			response.err = errors.New("no storage path set via STORAGE_PATH env variable")
			return
		}
		// Get the current time
		currentTime := time.Now().Format(time.RFC3339)

		// Write time to file
		err := os.WriteFile(storagePathEnv, []byte(currentTime), 0644)
		if err != nil {
			response.err = fmt.Errorf("error writing to file: %w", err)
			return
		}
		response.Message = fmt.Sprintf("current time written to store: %s", currentTime)
		return
	})

	http.HandleFunc("/read-storage", func(w http.ResponseWriter, r *http.Request) {
		response := &response{writer: w, logger: logger}
		defer response.handle()

		if storagePathEnv == "" {
			response.err = errors.New("no storage path set via STORAGE_PATH env variable")
			return
		}
		content, err := os.ReadFile(storagePathEnv)
		if err != nil {
			response.err = fmt.Errorf("error reading from file: %w", err)
			return
		}
		response.Message = fmt.Sprintf("last time written to store: %s", string(content))
		return
	})

	http.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		response := &response{writer: w, logger: logger}
		defer response.handle()

		initCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		auth := nfs4.AuthParams{
			Uid:         uint32(uid),
			Gid:         uint32(gid),
			MachineName: machineName,
		}
		client, err := nfs4.NewNfsClient(initCtx, server, auth)
		if err != nil {
			response.err = fmt.Errorf("error creating NFS client: %w", err)
			return
		}

		path := r.URL.Query().Get("path")
		if path == "" {
			path = "/"
		}

		entries, err := client.GetFileList(path)
		if err != nil {
			response.err = fmt.Errorf("Error reading dir %q: %v", path, err)
			return
		}
		response.Files = entries
	})

	srv := &http.Server{
		Addr:    prefixPort(listenPort),
		Handler: http.DefaultServeMux, // built-in global mux
	}

	go func() {
		logger.Info("serving", "nfs-server", server, "nfs-share", share, "listen-address", listenPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", slog.Any("error", err.Error()))
			cancel()
		}
	}()

	// block until shutdown
	<-ctx.Done()
	logger.Info("shutting down HTTP server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("error on HTTP server shutdown", slog.Any("error", err))
	}

	logger.Info("exited cleanly")
}

func prefixPort(port string) string {
	if strings.HasPrefix(port, ":") {
		return port
	}
	return ":" + port
}

type response struct {
	Pod     string          `json:"pod,omitempty"`
	Error   string          `json:"error,omitempty"`
	Message string          `json:"message,omitempty"`
	Files   []nfs4.FileInfo `json:"files,omitempty"`
	writer  http.ResponseWriter
	logger  *slog.Logger
	err     error
}

func (r *response) handle() {
	r.writer.Header().Set("Content-Type", "application/json")
	r.Pod = podNameEnv

	if r.err != nil {
		r.Error = "something went wrong"
		r.logger.Error(r.err.Error())
		r.writer.WriteHeader(http.StatusInternalServerError)
	} else {
		r.writer.WriteHeader(http.StatusOK)
	}
	if err := json.NewEncoder(r.writer).Encode(r); err != nil {
		r.logger.Error("can not JSON encode output", slog.Any("error", err))
	}
}
func setupSqlite(ctx context.Context, storagePath string, podName string, logger *slog.Logger, sqliteReadIntervalEnv string, sqliteWriteIntervalEnv string) (func() error, error) {
	sqliteReadInterval, err := time.ParseDuration(sqliteReadIntervalEnv)
	if err != nil {
		return nil, fmt.Errorf("can not parse sqlite read interval env variable: %w", err)
	}
	sqliteWriteInterval, err := time.ParseDuration(sqliteWriteIntervalEnv)
	if err != nil {
		return nil, fmt.Errorf("can not parse sqlite write interval env variable: %w", err)
	}

	db, err := sql.Open("sqlite3", storagePath)
	if err != nil {
		return nil, fmt.Errorf("can not open sqlite db: %w", err)
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pod TEXT NOT NULL,
            ts DATETIME NOT NULL,
            payload TEXT
        )`)
	if err != nil {
		return db.Close, fmt.Errorf("can not create schema: %w", err)
	}

	// write to sqlite
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				payload := fmt.Sprintf("rand=%d", rand.Intn(1000))
				_, err := db.Exec(
					"INSERT INTO entries (pod, ts, payload) VALUES (?, datetime('now'), ?)",
					podName, payload,
				)
				if err != nil {
					logger.Error("WRITE error", slog.Any("error", err))
				}
				time.Sleep(sqliteWriteInterval)
			}
		}
	}()

	// read from sqlite
	go func() {
		ticker := time.NewTicker(sqliteReadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var count int
				var lastPod, lastTS, lastPayload string
				err := db.QueryRow(
					"SELECT COUNT(*), (SELECT pod FROM entries ORDER BY id DESC LIMIT 1), (SELECT ts FROM entries ORDER BY id DESC LIMIT 1), (SELECT payload FROM entries ORDER BY id DESC LIMIT 1)",
				).Scan(&count, &lastPod, &lastTS, &lastPayload)
				if err != nil {
					logger.Error("read error", slog.Any("error", err))
				} else {
					logger.Info("successful read", "count", count, "lastPod", lastPod, "lastTimestamp", lastTS, "lastPayload", lastPayload)
				}
			}
		}
	}()
	return db.Close, nil
}
