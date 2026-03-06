package ui

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

// distFS stores the compiled dashboard assets
//
//go:embed all:dist
var distFS embed.FS

// Handler serves the embedded UI under /ui/. It serves static assets directly and falls back to index.html for SPA routes
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("ui dist directory is missing from embedded assets")
	}

	static := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/ui/")
		if rel == "" || rel == "/" {
			serveIndex(sub, w, r)
			return
		}

		clean := strings.TrimPrefix(path.Clean("/"+rel), "/")
		if clean == "" || clean == "." {
			serveIndex(sub, w, r)
			return
		}

		info, err := fs.Stat(sub, clean)
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

		serveIndex(sub, w, r)
	})
}

func serveIndex(fsys fs.FS, w http.ResponseWriter, _ *http.Request) {
	data, err := fs.ReadFile(fsys, "index.html")
	if err != nil {
		http.Error(w, "ui index not found", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
