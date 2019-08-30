package formats

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ErrUnsupportedFormat indicates that the file format is not supported.
var ErrUnsupportedFormat = errors.New("databio/formats: unsupported format")

// Open returns a Reader for the input file if it detects that it
// is in the supported Format. Returns ErrUnsupportedFormat is the Format is
// not detected.
func Open(in *os.File) (Reader, error) {
	info, err := in.Stat()
	if err != nil {
		return nil, err
	}

	ext := filepath.Ext(info.Name())
	switch strings.ToLower(ext) {
	case ".csv":
		return OpenCSV(in)
	case ".txt", ".tsv", ".tab":
		return OpenTSV(in)
	case ".xlsx":
		return OpenXLSX(in)
	}
	return nil, ErrUnsupportedFormat
}

// Reader returns tabular records from a supported Format.
type Reader interface {
	// Next returns the next Record in the document.
	Next() (*Record, error)

	// Err returns the last error that occured.
	Err() error
}

// Record represents a single record sourced from the Format.
type Record struct {
	// Fields contains the field names for each value.  NB for Formats that
	// can contain multiple values, Fields may contain duplicates.
	Fields []string

	// Values contains the values of each corresponding Field.
	Values []string
}

// Map returns a map of fields and values for the Record.
func (r *Record) Map() map[string][]string {
	res := make(map[string][]string, len(r.Values))
	for i, f := range r.Fields {
		res[f] = append(res[f], r.Values[i])
	}
	return res
}

// SingleMap returns a map of fields and singular values for the Record.
// Note if any fields/values have multiple entries only one is returned.
func (r *Record) SingleMap() map[string]string {
	res := make(map[string]string, len(r.Values))
	for i, f := range r.Fields {
		res[f] = r.Values[i]
	}
	return res
}
