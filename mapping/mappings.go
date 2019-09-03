package mapping

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joiningdata/databio"
	"github.com/joiningdata/databio/formats"
	"github.com/joiningdata/databio/sources"
)

// Mapper handles data mapping/translation tasks.
type Mapper struct {
	pump chan request
	src  *sources.Database
}

// NewMapper starts a new background processor and returns
// the newly created Mapper instance.
func NewMapper(s *sources.Database) *Mapper {
	m := &Mapper{
		pump: make(chan request, 4),
		src:  s,
	}
	go m.run()
	return m
}

type request struct {
	inputFilename string
	resultToken   string

	fromField string
	fromID    string
	toID      string
}

// Result describes the mapping process and results.
type Result struct {
	// Token for retrieving result metadata.
	Token string `json:"token"`
	// Log of the versions/timestamps of data and software used for mapping.
	Log string `json:"log"`
	// Methods contains a prose description of the process used to map values.
	Methods string `json:"methods"`
	// Citations contains a list of citations that match the Methods.
	Citations []string `json:"citations"`
	// NewFilename contains the filename for the output mapped CSV file.
	NewFilename string `json:"newfilename"`
}

// Start a new identifier mapping task in the background and return a job token.
func (m *Mapper) Start(fname, fromField, fromID, toID string) string {
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(fname)))
	d := request{
		inputFilename: fname,
		resultToken:   token,
		fromField:     fromField,
		fromID:        fromID,
		toID:          toID,
	}
	m.pump <- d
	return token
}

// Status checks for a Result using the given job-token.
func (m *Mapper) Status(token string) (res *Result, done bool) {
	res = &Result{}
	notready, err := databio.GetResult(token, "mapping", res)
	if notready {
		return nil, false
	}
	if err != nil {
		log.Println(err)
		return nil, true
	}
	return res, true
}

func (m *Mapper) run() {
	for req := range m.pump {
		res := &Result{Token: req.resultToken}
		ext := filepath.Ext(req.inputFilename)
		res.NewFilename = strings.Replace(req.inputFilename, ext, ".translated.csv", 1)

		translator, err := m.src.GetMapping(req.fromID, req.toID)
		if err != nil {
			log.Println("stage0", req, err)
			databio.PutResult(req.resultToken, "mapping",
				"error", "unable to get translator")
			continue
		}

		f, err := os.Open(databio.GetUploadPath(req.inputFilename))
		if err != nil {
			log.Println("stage1", req, err)
			databio.PutResult(req.resultToken, "mapping",
				"error", "unable to read input")
			continue
		}

		r, err := formats.Open(f)
		if err != nil {
			log.Println("stage2", req, err)
			databio.PutResult(req.resultToken, "mapping",
				"error", "unable to parse input")
			continue
		}

		fout, err := os.Create(databio.GetDownloadPath(res.NewFilename))
		if err != nil {
			log.Println("stage3", req, err)
			databio.PutResult(req.resultToken, "mapping",
				"error", "unable to create output")
			continue
		}
		defer fout.Close()
		csvwr := csv.NewWriter(fout)
		defer csvwr.Flush()

		numMissing := 0
		first := true
		rec, err := r.Next()
		for err == nil {
			missing := false
			row := rec.Map()
			vals, ok := row[req.fromField]
			if ok {
				for i, v := range vals {
					vals[i], ok = translator[v]
					if !ok {
						missing = true
					}
				}
				row[req.fromField] = vals
			}

			if missing {
				log.Println("missing data", rec)
				numMissing++

				rec, err = r.Next()
				continue
			}

			if first {
				first = false
				err = csvwr.Write(rec.Fields)
				if err != nil {
					log.Println("stage4", req, err)
					databio.PutResult(req.resultToken, "mapping",
						"error", "unable to write header to output")
					return
				}
			}

			line := make([]string, len(rec.Fields))
			for i, v := range rec.Fields {
				line[i] = strings.Join(row[v], "|")
			}
			err = csvwr.Write(line)
			if err != nil {
				log.Println("stage4", req, err)
				databio.PutResult(req.resultToken, "mapping",
					"error", "unable to write to output")
				return
			}

			rec, err = r.Next()
		}
		if err != io.EOF {
			log.Println("stage99", req, err)
			databio.PutResult(req.resultToken, "mapping",
				"error", "unable to translate")
			return
		}

		///////////////
		res.Log = "this is the timestamped and hashed logs for reproduction"
		res.Methods = "this is the methods text"
		res.Citations = []string{
			"this is a citation ready for import to endnote/zotero",
			"this is citation #2",
		}
		databio.PutResult(req.resultToken, "mapping", res)
	}
}
