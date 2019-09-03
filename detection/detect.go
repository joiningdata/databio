package detection

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"

	"github.com/joiningdata/databio"

	"github.com/joiningdata/databio/formats"
	"github.com/joiningdata/databio/sources"
)

const maxSamples = 5000

// Detector handles data format detection tasks.
type Detector struct {
	pump chan request
	src  *sources.Database
}

// NewDetector starts a new background processor and returns
// the newly created Detector instance.
func NewDetector(s *sources.Database) *Detector {
	d := &Detector{
		pump: make(chan request, 4),
		src:  s,
	}
	go d.run()
	return d
}

// Result encodes the results of a detection task on a data file.
type Result struct {
	// InputFilename is the source filename (relative to upload directory).
	InputFilename string `json:"input_file"`

	// DetectedSources reports, for each field of the input, the detected
	// data Sources and a percentage hit ratio.
	DetectedSources map[string]map[string]float64 `json:"detected"`

	// Types reports the detected data types of each field.
	Types map[string]string `json:"types"`

	// Maps reports the possible direct translation destinations for each
	// data source that was possibly detected in the input.
	Maps map[string][]string `json:"maps"`

	// Sources is the list of sources used for detection.
	Sources map[string]*sources.Source `json:"sources"`
}

type request struct {
	inputFilename string
	resultToken   string
}

///////////////

// Start a background detection process on the given filename.
// Returns a job token that can be used to check job status
func (d *Detector) Start(fname string) string {
	token := fmt.Sprintf("%x", sha256.Sum256([]byte(fname)))
	x := request{
		inputFilename: fname,
		resultToken:   token,
	}
	d.pump <- x
	return token
}

// Status checks for a Result using the given job-token.
func (d *Detector) Status(token string) (res *Result, done bool) {
	res = &Result{}
	notready, err := databio.GetResult(token, "detection", res)
	if notready {
		return nil, false
	}
	if err != nil {
		log.Println(err)
		return nil, true
	}
	return res, true
}

func (d *Detector) run() {
	for req := range d.pump {
		res := &Result{
			InputFilename: req.inputFilename,
			Sources:       d.src.Sources,
		}
		f, err := os.Open(databio.GetUploadPath(req.inputFilename))
		if err != nil {
			log.Println("stage0", req, err)
			databio.PutResult(req.resultToken, "detection",
				"error", "unable to read input")
			continue
		}

		r, err := formats.Open(f)
		if err != nil {
			log.Println("stage1", req, err)
			databio.PutResult(req.resultToken, "detection",
				"error", "unable to parse input")
			continue
		}

		///// collect a sample of the input records
		samples := make(map[string][]string)
		n := 0
		rec, err := r.Next()
		for err == nil {
			n++
			if n > maxSamples {
				break
			}
			rec.Each(func(colname, value string) error {
				samples[colname] = append(samples[colname], value)
				return nil
			})

			rec, err = r.Next()
		}

		///// determine if each column is numeric or text
		coltypes := make(map[string]string)
		pfxint := regexp.MustCompile("^[A-Za-z]*:[0-9]*$")
		for colname, sample := range samples {
			nIntegers := 0
			nFloats := 0
			nPrefixedIntegers := 0

			for _, s := range sample {
				_, err := strconv.ParseInt(s, 10, 64)
				if err == nil {
					nIntegers++
					nFloats++
					continue
				}
				_, err = strconv.ParseFloat(s, 64)
				if err == nil {
					nFloats++
					continue
				}

				///////////
				if pfxint.MatchString(s) {
					nPrefixedIntegers++
				}
			}

			if nFloats > nIntegers && nFloats > nPrefixedIntegers {
				coltypes[colname] = "floats"
			} else if nIntegers > nPrefixedIntegers {
				coltypes[colname] = "integers"
			} else if nPrefixedIntegers >= len(sample)/2 {
				coltypes[colname] = "prefixed integers"
			} else {
				coltypes[colname] = "text"
			}
		}

		////////////
		// try to classify each column's source
		colsrcs := make(map[string]map[string]float64)
		sourcemaps := make(map[string][]string)
		for colname, ctype := range coltypes {
			sample := samples[colname]
			sources := d.identify(ctype, sample)
			colsrcs[colname] = sources

			for s := range sources {
				if _, ok := sourcemaps[s]; ok {
					continue
				}
				sourcemaps[s] = d.src.Mappings(s)
			}
		}

		res.DetectedSources = colsrcs
		res.Maps = sourcemaps
		res.Types = coltypes

		databio.PutResult(req.resultToken, "detection", res)
	}
}
