package formats

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	// ErrUnsupportedFormat indicates that the file format is not supported.
	ErrUnsupportedFormat = errors.New("databio/formats: unsupported format")

	// ErrWriterNotSupported is returned when Writer is not implemented for a Format.
	ErrWriterNotSupported = errors.New("databio/formats: Writer not supported for this format")
)

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

// Reader returns Records from a supported Format.
type Reader interface {
	// Next returns the next Record in the document.
	Next() (Record, error)

	// Err returns the last error that occured.
	Err() error
}

// Writer serializes records to a supported Format.
type Writer interface {
	// Write serializes the Record.
	Write(Record) error

	// Err returns the last error that occured.
	Err() error
}

// Record represents a single record sourced from the Format.
type Record interface {
	// Each iterates over every field/value pair in the Record.
	Each(func(field, value string) error) error

	// Fields returns the a list of Fields present in this Record.
	Fields() []string

	// Values returns the values associated with a named Field.
	Values(field string) []string

	// Set the values for a named Field in the Record.
	Set(field string, values []string)
}

type simpleRec struct {
	// Fields contains the field names for each value.
	fields []string

	// Values contains the values of each corresponding Field.
	values [][]string
}

func (x *simpleRec) Each(cb func(field, value string) error) error {
	var err error
	for i, vals := range x.values {
		f := x.fields[i]
		for _, val := range vals {
			cb(f, val)
		}
	}
	return err
}

func (x *simpleRec) Fields() []string {
	return x.fields
}

func (x *simpleRec) Values(field string) []string {
	for i, f := range x.fields {
		if i >= len(x.values) {
			return nil
		}
		if f == field {
			return x.values[i]
		}
	}
	return nil
}

func (x *simpleRec) Set(field string, vals []string) {
	for i, f := range x.fields {
		if f == field {
			x.values[i] = vals
			return
		}
	}
	x.fields = append(x.fields, field)
	x.values = append(x.values, vals)
}

///////////

// Format describes a supported data interchange protocol.
type Format struct {
	// Name of the Format used for locating the Reader/Writer to use.
	Name string

	// Description of the Format used for selection lists.
	Description string

	// Extensions lists the file extensions that typicall denote this Format.
	// Note each extension MUST begin with a "." dot prefix.
	Extensions []string

	// MediaTypes lists the IANA Media/MIME types supported by the Format.
	MediaTypes []string

	// Detect if the given (possibly incomplete) data is supported.
	//    Supported = true if this Format will work for the data.
	//    More = true if more data may help detection.
	// Note that if Supported=false, More=true and you have provided
	// the entire contents then the data format is either truncated
	// or not supported.
	Detect func(data []byte, incomplete bool) (supported bool, more bool)

	// NewReader returns a new format Reader for the given stream.
	NewReader func(r io.Reader) (Reader, error)

	// NewWriter returns a new format Writer applied to the given stream.
	NewWriter func(w io.Writer) (Writer, error)
}

// Register a Format for inclusion in any subsequent data import/export tasks.
// Returns the number of formats currently registered, thus it can be used as
// a global initializer by ignoring the result:
//
//    var _ = formats.Register(formats.Format{...})
//
func Register(f *Format) int {
	_, ok := supportedFormats[f.Name]
	if ok {
		panic("the format '" + f.Name + "' is already in use.")
	}
	supportedFormats[f.Name] = f
	return len(supportedFormats)
}

func writerNotSupported(w io.Writer) (Writer, error) {
	return nil, ErrWriterNotSupported
}

var (
	supportedFormats = make(map[string]*Format)
)
