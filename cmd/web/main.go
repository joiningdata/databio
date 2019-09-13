// Command web runs the web service that handles user interactions:
//   uploads, detection reporting, and translation/downloads
package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/joiningdata/databio"

	"github.com/gorilla/sessions"
	"github.com/joiningdata/databio/detection"
	"github.com/joiningdata/databio/mapping"
	"github.com/joiningdata/databio/sources"
)

const (
	databioSessionName = "databio-session"
	maxUploadBytes     = 32 << 20 // 32MB

	uploadBase = "uploads"
)

var (
	store     = sessions.NewCookieStore([]byte(os.Getenv("SESSION_KEY")))
	templates *template.Template

	srcDB    *sources.Database
	detector *detection.Detector
	mapper   *mapping.Mapper
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("index.html", templates.ExecuteTemplate(w, "index.html", nil))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := store.Get(r, databioSessionName)

	if r.Method != http.MethodPost {
		http.Error(w, "upload missing", http.StatusBadRequest)
		return
	}

	err := r.ParseMultipartForm(maxUploadBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fhs, ok := r.MultipartForm.File["data"]
	if !ok {
		http.Error(w, "upload missing", http.StatusBadRequest)
		return
	}

	// sanitize the filename and extension
	fname := filepath.Base(fhs[0].Filename)
	fext := strings.ToLower(filepath.Ext(fname))
	fext = regexp.MustCompile("[^a-z0-9]*").ReplaceAllString(fext, "")
	fname = fmt.Sprintf("%x", sha256.Sum256([]byte(fname)))
	if fext != "" {
		fname += "." + fext
	}

	fin, err := fhs[0].Open()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fout, err := os.Create(databio.GetUploadPath(fname))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(fout, fin)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fout.Close()
	fin.Close()

	session.Values["documentKey"] = fname
	token := detector.Start(fname)

	session.Save(r, w)
	http.Redirect(w, r, "/report?k="+token, http.StatusSeeOther)
}

func reportHandler(w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	token := r.URL.Query().Get("k")
	if token == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	session.Values["det_token"] = token
	session.Save(r, w)

	ctx, done := detector.Status(token)
	if !done {
		w.Header().Set("Refresh", "1;url=/report?k="+token)
		fmt.Fprint(w, "Please wait...")
		return
	} else if ctx == nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("report.html", templates.ExecuteTemplate(w, "report.html", ctx))
}

func quickmapHandler(w http.ResponseWriter, r *http.Request) {
	_, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	r.ParseForm()
	fromID := r.Form.Get("from")
	toID := r.Form.Get("to")
	idlist := strings.Split(strings.TrimSpace(r.Form.Get("ids")), "\n")

	translator, err := srcDB.GetMapper(fromID, toID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dest := srcDB.Sources[toID]
	for _, id := range idlist {
		id = strings.TrimSpace(id)
		if id == "" {
			//fmt.Fprintln(w)
			continue
		}
		id2s, ok := translator.Get(id)
		if !ok || len(id2s) == 0 {
			fmt.Fprintf(w, "(missing %s='%s')\n", fromID, id)
		} else {
			for i, id2 := range id2s {
				if i > 0 {
					fmt.Fprintf(w, ", ")
				}
				fmt.Fprintf(w, `<a href="%s">%s</a>`, dest.Linkout(id2), id2)
			}
			fmt.Fprintf(w, "\n")
		}
	}
}

func translateHandler(w http.ResponseWriter, r *http.Request) {
	_, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	q := r.URL.Query()
	fname := q.Get("doc")
	b64field := q.Get("field")
	fb, err := base64.RawURLEncoding.DecodeString(b64field)
	if err != nil {
		log.Println(err)
		http.Error(w, "invalid field", http.StatusBadRequest)
		return
	}
	fromField := string(fb)
	fromID := q.Get("from")
	toID := q.Get("to")

	log.Println("Document: ", fname)
	log.Println("Translate from", fromField, "/", fromID, "to", toID)

	token := mapper.Start(fname, &mapping.Options{
		FromField:    fromField,
		FromSource:   fromID,
		ToSource:     toID,
		Replace:      true,
		DropMissing:  true,
		OutputFormat: "csv",
	})

	http.Redirect(w, r, "/wait?k="+token, http.StatusSeeOther)
}

func waitHandler(w http.ResponseWriter, r *http.Request) {
	_, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	token := r.URL.Query().Get("k")
	if token == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	//session.Save(r, w)

	ctx, done := mapper.Status(token)
	if !done {
		w.Header().Set("Refresh", "1;url=/wait?k="+token)
		fmt.Fprint(w, "Please wait...")
		return
	} else if ctx == nil {
		http.Error(w, "no context", http.StatusInternalServerError)
		return
	}

	ctx.Methods = strings.Replace(ctx.Methods, "\n", "<br/>\n", -1)

	log.Println("ready.html", templates.ExecuteTemplate(w, "ready.html", ctx))
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	_, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	token := r.URL.Query().Get("k")
	if token == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	/////////////////////////
	info, done := mapper.Status(token)
	if !done {
		http.Redirect(w, r, "/wait", http.StatusSeeOther)
		return
	} else if info == nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-type", "application/zip")

	zw := zip.NewWriter(w)
	zwf, err := zw.Create("databio.log")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(zwf, info.Log)
	zwf, err = zw.Create("stats.json")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jb, _ := json.MarshalIndent(info.Stats, "", "    ")
	zwf.Write(jb)
	zwf, err = zw.Create("methods.txt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(zwf, info.Methods)
	zwf, err = zw.Create("citations.ris")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(zwf, strings.Join(info.Citations, "\r\n"))

	zwf, err = zw.Create(info.NewFilename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f, err := os.Open(databio.GetDownloadPath(info.NewFilename))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = io.Copy(zwf, f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.Close()
	zw.Close()
}

// http://localhost:8080/translate?field=R2VuZV9JRA&from=gov.nih.nlm.ncbi.gene&to=org.ensembl.gene

func main() {
	dbname := flag.String("db", "sources.sqlite", "database `filename` to load source datasets")
	addr := flag.String("i", ":8080", "`address:port` to listen for web requests")
	flag.Parse()

	err := databio.CheckDirectories()
	if err != nil {
		log.Fatal(err)
	}

	srcDB, err = sources.Open(*dbname)
	if err != nil {
		log.Fatal(err)
	}
	detector = detection.NewDetector(srcDB)
	mapper = mapping.NewMapper(srcDB)

	templates = template.New("databio")
	templates.Funcs(template.FuncMap{
		"b64": func(src string) string {
			return base64.RawURLEncoding.EncodeToString([]byte(src))
		},
		"join": func(src []string) string {
			return strings.Join(src, "\n")
		},
		"pct": func(v float64) string {
			return fmt.Sprintf("%0.2f%%", v*100.0)
		},
		"safe": func(h string) template.HTML {
			return template.HTML(h)
		},
		"joinLinkout": func(src *sources.Source, data []string) template.HTML {
			res := make([]string, len(data))
			for i, x := range data {
				res[i] = `<a href="` + src.Linkout(x) + `">` + x + "</a>"
			}
			return template.HTML(strings.Join(res, "\n"))
		},
	})
	templates, err = templates.ParseGlob("templates/*.html")
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))

	http.HandleFunc("/", indexHandler)              // index.html => POST to /upload
	http.HandleFunc("/upload", uploadHandler)       // file upload => redirect to /report
	http.HandleFunc("/report", reportHandler)       // report.html => POST to /translate
	http.HandleFunc("/quickmap", quickmapHandler)   // in-page quick translation call for mapping preview
	http.HandleFunc("/translate", translateHandler) // begin translation => redirect to /wait
	http.HandleFunc("/wait", waitHandler)           // translate.html => GET to /download
	http.HandleFunc("/download", downloadHandler)   // package ZIP file
	log.Println("Listening on", *addr)
	log.Println(http.ListenAndServe(*addr, nil))
}
