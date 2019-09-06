package mapping

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
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

	// TotalRecords counts the total number of records processed.
	TotalRecords int `json:"total_records"`

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
		m.runOne(req)
	}
}

func (m *Mapper) runOne(req request) {
	opts := req.options
	res := &Result{Token: req.resultToken}
	stats := &Stats{StartTime: time.Now()}
	ext := filepath.Ext(req.inputFilename)
	res.NewFilename = strings.Replace(req.inputFilename, ext, ".translated.csv", 1)

	if opts.OutputFormat != "csv" {
		log.Println("stage0", req, req.options.OutputFormat)
		databio.PutResult(req.resultToken, "mapping",
			"error", "only csv output is currently supported")
		return
	}

	translator, err := m.src.GetMapper(opts.FromSource, opts.ToSource)
	if err != nil {
		log.Println("stage0", req, err)
		databio.PutResult(req.resultToken, "mapping",
			"error", "unable to get translator")
		return
	}

	f, err := os.Open(databio.GetUploadPath(req.inputFilename))
	if err != nil {
		log.Println("stage1", req, err)
		databio.PutResult(req.resultToken, "mapping",
			"error", "unable to read input")
		return
	}
	defer f.Close()

	r, err := formats.Open(f)
	if err != nil {
		log.Println("stage2", req, err)
		databio.PutResult(req.resultToken, "mapping",
			"error", "unable to parse input")
		return
	}

	fout, err := os.Create(databio.GetDownloadPath(res.NewFilename))
	if err != nil {
		log.Println("stage3", req, err)
		databio.PutResult(req.resultToken, "mapping",
			"error", "unable to create output")
		return
	}
	csvwr := csv.NewWriter(fout)

	newFieldName := opts.FromField
	if !opts.Replace {
		newFieldName = m.src.Sources[opts.ToSource].Name
	}
	first := true
	rec, err := r.Next()
	for err == nil {
		missing := false
		multiple := false
		vals := rec.Values(opts.FromField)
		stats.TotalRecords++
		if len(vals) > 0 {
			v2 := make([]string, 0, len(vals))
			for _, v := range vals {
				vx, ok := translator.Get(v)
				if !ok || len(vx) == 0 {
					missing = true
					stats.SourceMissingValues++
				}
				if len(vx) > 1 {
					multiple = true
					stats.DestinationMultipleValues++
					stats.DestinationMultipleNewCount += len(vx) - 1
				}
				v2 = append(v2, vx...)
			}
			rec.Set(newFieldName, v2)

			if multiple {
				stats.DestinationMultipleRecords++
			}
		}

		if missing {
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
				csvwr.Flush()
				fout.Close()
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
			csvwr.Flush()
			fout.Close()
			return
		}

		rec, err = r.Next()
	}
	csvwr.Flush()
	fout.Sync()
	uploadInfo, _ := f.Stat()
	convertedInfo, _ := fout.Stat()
	fout.Close()

	if err != io.EOF {
		log.Println("stage99", req, err)
		databio.PutResult(req.resultToken, "mapping",
			"error", "unable to translate")
		return
	}

	///////////////
	stats.EndTime = time.Now()
	res.Stats = stats

	fmtArgs := []interface{}{
		m.src.Sources[opts.FromSource].Description, 1,
		m.src.Sources[opts.ToSource].Description, 2,
		3,
	}
	res.Methods = "Source identifiers were recognized as %ss [%d], and were " +
		"converted to %ss [%d] using the Databio tools [%d]. "

	t1 := m.src.Sources[opts.FromSource].LastUpdate
	t2 := m.src.Sources[opts.ToSource].LastUpdate

	uploadSize := fmt.Sprintf("(%d byte %s)", uploadInfo.Size(), filepath.Ext(uploadInfo.Name()))
	convertedSize := fmt.Sprintf("(%d byte %s)", convertedInfo.Size(), filepath.Ext(convertedInfo.Name()))

	logs := []string{
		"- date/times in UTC - Processed using data integration tools at https://datab.io",
		t1.Format("2006-01-02 15:04:05") + " - Data fetched for " + m.src.Sources[opts.FromSource].Description,
		t2.Format("2006-01-02 15:04:05") + " - Data fetched for " + m.src.Sources[opts.ToSource].Description,
		uploadInfo.ModTime().UTC().Format("2006-01-02 15:04:05") + " - Source data uploaded to Databio " + uploadSize,
		convertedInfo.ModTime().UTC().Format("2006-01-02 15:04:05") + " - Data mapping completed " + convertedSize,
	}
	sort.Strings(logs)

	if t2.Before(t1) {
		t1 = t2
	}

	if stats.SourceMissingValues > 0 {
		res.Methods += "This conversion resulted in the loss of %d/%d (%3.2f%%) source identifiers, " +
			"likely due to database changes that occured between original distribution " +
			"and the mapping data (sourced on %s). "
		fmtArgs = append(fmtArgs, stats.SourceMissingValues, stats.TotalRecords,
			float64(stats.SourceMissingValues)*100.0/float64(stats.TotalRecords),
			t1.Format("2 January, 2006"))
	} else {
		res.Methods += "The mapping data used for identifier conversion was sourced on %s. "
		fmtArgs = append(fmtArgs, t1.Format("2 January, 2006"))
	}

	if stats.DestinationMultipleRecords > 0 {
		res.Methods += "Because of ambiguity between the identifier types, %d/%d (%3.2f%%) %ss were " +
			"expanded to include multiple associated %ss each. "
		fmtArgs = append(fmtArgs, stats.DestinationMultipleRecords, stats.TotalRecords,
			float64(stats.DestinationMultipleRecords)*100.0/float64(stats.TotalRecords),
			m.src.Sources[opts.FromSource].Description,
			m.src.Sources[opts.ToSource].Description)
	}

	fmtArgs = append(fmtArgs,
		m.src.Sources[opts.FromSource].Cite(),
		m.src.Sources[opts.ToSource].Cite(),
		databioCitations[0])
	res.Methods += "\n\n  1. %s\n  2. %s\n  3. %s"
	res.Methods = fmt.Sprintf(res.Methods, fmtArgs...)

	res.Citations = []string{
		m.src.Sources[opts.FromSource].Citation,
		m.src.Sources[opts.ToSource].Citation,
		databioCitations[1],
	}
	res.Log = strings.Join(logs, "\n")
	databio.PutResult(req.resultToken, "mapping", res)
}

// TODO: FIXME: actually publish something
var databioCitations = []string{
	`Jay et al. "Automated Data Integration tools for reproducible research" In prep. (2019).`,

	strings.Replace(`TY  - JOUR
		TI  - Automated Data Integration tools for reproducible research.
		AU  - Jay, Jeremy J
		T2  - In preparation
		PY  - 2019
		J2  - In prep
		ER  - 
		`, "\n\t\t", "\r\n", -1),
}
