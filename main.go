package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
	server      = os.Getenv("NFS_SERVER") // e.g. "192.168.1.50"
	share       = os.Getenv("NFS_SHARE")  // e.g. "/export/data"
	listenPort  = os.Getenv("PORT")
	timeoutEnv  = os.Getenv("TIMEOUT")
	machineName = os.Getenv("MACHINE_NAME")
	uidEnv      = os.Getenv("UID")
	gidEnv      = os.Getenv("GID")
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

	initCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	auth := nfs4.AuthParams{
		Uid:         uint32(uid),
		Gid:         uint32(gid),
		MachineName: machineName,
	}
	client, err := nfs4.NewNfsClient(initCtx, server, auth)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")
		if path == "" {
			path = "."
		}

		entries, err := client.GetFileList(path)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading dir %q: %v", path, err), http.StatusInternalServerError)
			return
		}

		out := []nfs4.FileInfo{}
		for _, e := range entries {
			out = append(out, e)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	log.Printf("Serving on %s (NFS %s:%s)", listenPort, server, share)
	if err := http.ListenAndServe(listenPort, nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
