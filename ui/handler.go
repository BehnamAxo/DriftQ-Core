package ui

import (
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Handler serves the dashboard UI under /ui/ when dist assets are available on disk.
func Handler() http.Handler {
	if disk, ok := diskDistFS(); ok {
		return spaHandler(disk)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "ui assets are not available", http.StatusNotFound)
	})
}

func diskDistFS() (fs.FS, bool) {
	candidates := make([]string, 0, 5)

	if env := strings.TrimSpace(os.Getenv("DRIFTQ_UI_DIST")); env != "" {
		candidates = append(candidates, env)
	}

	candidates = append(candidates,
		filepath.Join("ui", "dist"),
		"dist",
		filepath.Join(string(filepath.Separator), "ui", "dist"),
	)

	if exe, err := os.Executable(); err == nil {
		base := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(base, "ui", "dist"),
			filepath.Join(base, "dist"),
		)
	}

	for _, dir := range candidates {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		return os.DirFS(dir), true
	}

	return nil, false
}

func spaHandler(dist fs.FS) http.Handler {
	static := http.FileServer(http.FS(dist))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/ui/")
		if rel == "" || rel == "/" {
			serveIndex(dist, w)
			return
		}

		clean := strings.TrimPrefix(path.Clean("/"+rel), "/")
		if clean == "" || clean == "." {
			serveIndex(dist, w)
			return
		}

		info, err := fs.Stat(dist, clean)
		if err == nil && !info.IsDir() {
			// Keep JS MIME type explicit for older proxies.
			if path.Ext(clean) == ".js" {
				w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			} else if ext := path.Ext(clean); ext != "" {
				if ct := mime.TypeByExtension(ext); ct != "" {
					w.Header().Set("Content-Type", ct)
				}
			}

			r2 := r.Clone(r.Context())
			r2.URL.Path = "/" + clean
			static.ServeHTTP(w, r2)
			return
		}

		serveIndex(dist, w)
	})
}

func serveIndex(fsys fs.FS, w http.ResponseWriter) {
	data, err := fs.ReadFile(fsys, "index.html")
	if err != nil {
		http.Error(w, "ui index not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
