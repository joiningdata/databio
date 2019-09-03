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
	"time"

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
	options       *Options
}

// Options records various mapping parameters to control the process.
type Options struct {
	// FromField indicates the input Field in each record used for Mapping.
	FromField string

	// FromSource indicates the source for identifiers in FromField.
	FromSource string

	// ToSource indicates the source to map identifiers to.
	ToSource string

	// Replace is true if the values should be replaced in-place,
	// false if the mapped values should be appended.
	Replace bool

	// DropMissing indicates that rows that could not be mapped should be
	// dropped from output. If false, empty values are used.
	DropMissing bool

	// OutputFormat describes the requested output format.
	OutputFormat string
}

// Result describes the mapping process and results.
type Result struct {
	// Token for retrieving result metadata.
	Token string `json:"token"`

	// Options that were used to drive the mapping process.
	Options *Options `json:"options"`

	// Log of the versions/timestamps of data and software used for mapping.
	Log string `json:"log"`
	// Methods contains a prose description of the process used to map values.
	Methods string `json:"methods"`
	// Citations contains a list of citations that match the Methods.
	Citations []string `json:"citations"`
	// NewFilename contains the filename for the output mapped CSV file.
	NewFilename string `json:"newfilename"`

	// Stats for how the mapping went.
	Stats *Stats `json:"stats"`
}

// Stats describes various metrics for how the mapping went.
type Stats struct {
	// StartTime of the mapping process.
	StartTime time.Time `json:"start_time"`

	// EndTime of the mapping process.
	EndTime time.Time `json:"end_time"`

	// SourceMissingRecords counts the number of records that contained a
	// Source ID that was not in the mapping dataset.
	SourceMissingRecords int `json:"source_missing_records"`

	// SourceMissingValues is similar to SourceMissingRecords but instead
	// counts the number of unique values missing.
	SourceMissingValues int `json:"source_missing_values"`

	// DestinationMultipleRecords counts the number of input records that
	// contained a Source ID that had multiple mapped values each.
	DestinationMultipleRecords int `json:"destination_multiple_records"`

	// DestinationMultipleValues is similar to DestinationMultipleRecords
	// but instead counts the number of unique values with multiple hits.
	DestinationMultipleValues int `json:"destination_multiple_values"`

	// DestinationMultipleNewCount counts the number of new records added
	// as a result of multiple mappings.
	DestinationMultipleNewCount int `json:"destination_multiple_new"`
}

// Start a new identifier mapping task in the background and return a job token.
func (m *Mapper) Start(fname string, opts *Options) string {
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(fname)))
	d := request{
		inputFilename: fname,
		resultToken:   token,
		options:       opts,
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
		opts := req.options
		res := &Result{Token: req.resultToken}
		stats := &Stats{StartTime: time.Now()}
		ext := filepath.Ext(req.inputFilename)
		res.NewFilename = strings.Replace(req.inputFilename, ext, ".translated.csv", 1)

		if opts.OutputFormat != "csv" {
			log.Println("stage0", req, req.options.OutputFormat)
			databio.PutResult(req.resultToken, "mapping",
				"error", "only csv output is currently supported")
			continue
		}

		translator, err := m.src.GetMapping(opts.FromSource, opts.ToSource)
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

		newFieldName := opts.FromField
		if !opts.Replace {
			newFieldName = m.src.Sources[opts.ToSource].Name
		}
		first := true
		rec, err := r.Next()
		for err == nil {
			missing := false
			ok := false
			//row := rec.Map()
			vals := rec.Values(opts.FromField)
			if len(vals) > 0 {
				v2 := make([]string, len(vals))
				for i, v := range vals {
					v2[i], ok = translator[v]
					if !ok {
						missing = true
						stats.SourceMissingValues++
					}
				}
				rec.Set(newFieldName, v2)
			}

			if missing {
				log.Println("missing data", rec)
				stats.SourceMissingRecords++
				if opts.DropMissing {
					rec, err = r.Next()
					continue
				}
			}

			if first {
				first = false
				err = csvwr.Write(rec.Fields())
				if err != nil {
					log.Println("stage4", req, err)
					databio.PutResult(req.resultToken, "mapping",
						"error", "unable to write header to output")
					return
				}
			}

			line := make([]string, len(rec.Fields()))
			for i, v := range rec.Fields() {
				line[i] = strings.Join(rec.Values(v), "|")
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
		stats.EndTime = time.Now()
		res.Stats = stats
		res.Log = "this is the timestamped and hashed logs for reproduction"
		res.Methods = "this is the methods text"
		res.Citations = []string{
			"this is a citation ready for import to endnote/zotero",
			"this is citation #2",
		}
		databio.PutResult(req.resultToken, "mapping", res)
	}
}
