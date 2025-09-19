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
	"strconv"
	"strings"
	"time"

	"github.com/Cyberax/go-nfs-client/nfs4"
)

type FileInfo struct {
	Name  string    `json:"name"`
	IsDir bool      `json:"is_dir"`
	Size  uint64    `json:"size"`
	Mtime time.Time `json:"mtime"`
}

var (
	server         = os.Getenv("NFS_SERVER") // e.g. "192.168.1.50"
	share          = os.Getenv("NFS_SHARE")  // e.g. "/export/data"
	listenPort     = os.Getenv("PORT")
	timeoutEnv     = os.Getenv("TIMEOUT")
	machineName    = os.Getenv("MACHINE_NAME")
	uidEnv         = os.Getenv("UID")
	gidEnv         = os.Getenv("GID")
	storagePathEnv = os.Getenv("STORAGE_PATH")
	podNameEnv     = os.Getenv("POD_NAME")
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

	logger := slog.Default()

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

	logger.Info("serving", "nfs-server", server, "nfs-share", share, "listen-address", listenPort)
	if err := http.ListenAndServe(prefixPort(listenPort), nil); err != nil {
		logger.Error("HTTP server error", "error", err.Error())
		os.Exit(1)
	}
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
		r.logger.Error("can not JSON encode output", "error", err.Error())
	}
}
