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
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gorilla/sessions"
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
)

type DetectionInfo struct {
	Sources map[string]map[string]float64 `json:"sources"`
	Types   map[string]string             `json:"types"`
	Maps    map[string][]string           `json:"maps"`
}

type MappingResult struct {
	Log           string   `json:"log"`
	Methods       string   `json:"methods"`
	Citations     []string `json:"citations"`
	NewFilename   string   `json:"newfilename"`
	LocalFilename string   `json:"_localfilename"`
}

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
	fout, err := os.Create(filepath.Join(uploadBase, fname))
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

	fp1 := filepath.Join(uploadBase, fname)
	fp2 := filepath.Join(uploadBase, fname+".detection.json")
	startDetection(fp1, fp2)

	session.Save(r, w)
	http.Redirect(w, r, "/report?k="+fname, http.StatusSeeOther)
}

func reportHandler(w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	dockey := r.URL.Query().Get("k")
	if dockey == "" {
		dki, ok := session.Values["documentKey"]
		if ok {
			dockey = dki.(string)
		}
	}
	if dockey == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	log.Println("Document Key: ", dockey)
	//session.Save(r, w)

	b, err := ioutil.ReadFile(filepath.Join(uploadBase, dockey+".detection.json"))
	if err != nil {
		if os.IsNotExist(err) {
			w.Header().Set("Refresh", "1;url=/report")
			fmt.Fprint(w, "Please wait...")
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	//w.Header().Set("Content-type", "application/json")
	//w.Write(b)
	ctx := DetectionInfo{}
	json.Unmarshal(b, &ctx)
	log.Println("report.html", templates.ExecuteTemplate(w, "report.html", ctx))
}

func translateHandler(w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	q := r.URL.Query()
	fb, _ := base64.URLEncoding.DecodeString(q.Get("field"))
	fromField := string(fb)
	fromID := q.Get("from")
	toID := q.Get("to")

	dockey, ok := session.Values["documentKey"]
	if !ok {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	fname, ok := dockey.(string)
	log.Println("Document Key: ", fname)
	log.Println("translate from", fromField, "/", fromID, "to", toID)
	//session.Save(r, w)

	fp1 := filepath.Join(uploadBase, fname)
	fp2 := filepath.Join(uploadBase, fname+".translation.json")
	startTranslation(fp1, fp2, fromField, fromID, toID)

	session.Save(r, w)
	http.Redirect(w, r, "/wait?k="+fname, http.StatusSeeOther)
}

func waitHandler(w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	dockey := r.URL.Query().Get("k")
	if dockey == "" {
		dki, ok := session.Values["documentKey"]
		if ok {
			dockey = dki.(string)
		}
	}
	if dockey == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	log.Println("Document Key: ", dockey)
	//session.Save(r, w)

	b, err := ioutil.ReadFile(filepath.Join(uploadBase, dockey+".translation.json"))
	if err != nil {
		if os.IsNotExist(err) {
			w.Header().Set("Refresh", "1;url=/wait")
			fmt.Fprint(w, "Please wait...")
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	//w.Header().Set("Content-type", "application/json")
	//w.Write(b)

	ctx := MappingResult{}
	json.Unmarshal(b, &ctx)
	log.Println("ready.html", templates.ExecuteTemplate(w, "ready.html", ctx))
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	session, err := store.Get(r, databioSessionName)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	dockey := r.URL.Query().Get("k")
	if dockey == "" {
		dki, ok := session.Values["documentKey"]
		if ok {
			dockey = dki.(string)
		}
	}
	if dockey == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	log.Println("Document Key: ", dockey)

	/////////////////////////
	// new translated filename, stamped log, methods text, citations
	b, err := ioutil.ReadFile(filepath.Join(uploadBase, dockey+".translation.json"))
	if err != nil {
		if os.IsNotExist(err) {
			http.Redirect(w, r, "/wait", http.StatusSeeOther)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	var info MappingResult
	err = json.Unmarshal(b, &info)
	if err != nil {
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
	zwf, err = zw.Create("methods.txt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(zwf, info.Methods)
	zwf, err = zw.Create("citations.txt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(zwf, strings.Join(info.Citations, "\n\n"))

	zwf, err = zw.Create(info.NewFilename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f, err := os.Open(info.LocalFilename)
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

	var err error
	srcDB, err = sources.Open(*dbname)
	if err != nil {
		log.Fatal(err)
	}

	templates = template.New("databio")
	templates.Funcs(template.FuncMap{
		"b64": func(src string) string { return base64.URLEncoding.EncodeToString([]byte(src)) },
	})
	templates, err = templates.ParseGlob("templates/*.html")
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))

	http.HandleFunc("/", indexHandler)              // index.html => POST to /upload
	http.HandleFunc("/upload", uploadHandler)       // file upload => redirect to /report
	http.HandleFunc("/report", reportHandler)       // report.html => POST to /translate
	http.HandleFunc("/translate", translateHandler) // begin translation => redirect to /wait
	http.HandleFunc("/wait", waitHandler)           // translate.html => GET to /download
	http.HandleFunc("/download", downloadHandler)   // package ZIP file
	log.Println(http.ListenAndServe(*addr, nil))
}
