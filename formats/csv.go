package formats

import (
	"bytes"
	"encoding/csv"
	"io"
	"strings"
)

const (
	// check at most 5000 rows for header content
	csvHeaderCheckMaxRows = 5000

	csvMultiSplit = ";"
)

var (
	_ = Register(&Format{
		Name:        "CSV",
		Description: "Comma-separated Values",
		Extensions:  []string{".csv"},
		MediaTypes:  []string{"text/csv"},
		Detect:      detectCSV,
		NewReader: func(r io.Reader) (Reader, error) {
			// TODO: this'll panic if necessary, but we could do it cleaner later
			return OpenCSV(r.(io.ReadSeeker))
		},
		NewWriter: writerNotSupported,
	})
)

func detectCSV(data []byte, incomplete bool) (supported, more bool) {
	defer func() {
		if e := recover(); e != nil {
			supported = false
			more = false
		}
	}()

	if incomplete {
		// since we only get a chunk, make sure the last line is a full record
		idx := bytes.LastIndexByte(data, '\n')
		if idx == -1 {
			// we don't even have a full line if it is CSV...
			return false, true
		}
		data = data[:idx]
	}

	b := bytes.NewReader(data)
	r := csv.NewReader(b)
	// don't validate number of columns in case there's a weird header
	r.FieldsPerRecord = -1
	r.ReuseRecord = true
	colcounts := make(map[int]int)
	ncols := 0
	nlines := 0
	rec, err := r.Read()
	for ; err == nil; rec, err = r.Read() {
		ncols = len(rec)
		colcounts[ncols]++
		nlines++
	}
	if err == io.EOF {
		if ncols > 1 && colcounts[ncols] >= 2 {
			// we got 2 or more lines of the same number of columns as the last line
			// so it's probably CSV, more data likely won't help
			return true, false
		}

		// if ncols==1, we can just use the (faster) tab-delimited parser
		if nlines > 10 {
			// more than 10 lines read and the last 2 didn't match?
			// probably not CSV
			return false, false
		}

		// not enough lines to know yet, more data might help
		return false, true
	}

	// probably not in CSV
	return false, false
}

// CSV supports reading tabular records from an csv file.
type CSV struct {
	f io.ReadSeeker
	r *csv.Reader

	head []string

	stickyErr error
}

// OpenCSV opens a csv document and returns a formats.Reader.
// An io.ReadSeeker is required due to header detection readahead.
func OpenCSV(in io.ReadSeeker) (*CSV, error) {
	r := csv.NewReader(in)

	x := &CSV{
		f: in,
		r: r,
	}

	x.skipHeaders()

	return x, x.stickyErr
}

func (x *CSV) skipHeaders() {
	// if there are descriptive lines etc at the top we try to skip over them
	cols, err := x.r.Read()
	if err != nil {
		x.stickyErr = err
		return
	}

	// out of the first N rows, which number of columns is the most frequent?
	bestcols := 0
	nr := 0
	colcounts := make(map[int]int)
	for err == nil {
		nr++
		colcounts[len(cols)]++
		if colcounts[len(cols)] > colcounts[bestcols] {
			bestcols = len(cols)
		}
		if nr > csvHeaderCheckMaxRows {
			break
		}
		cols, err = x.r.Read()
	}
	if err != nil {
		if err != io.EOF {
			x.stickyErr = err
			return
		}
	}

	// TODO: verify that it actually looks like a CSV

	_, x.stickyErr = x.f.Seek(0, io.SeekStart)
	if x.stickyErr != nil {
		return
	}
	x.r = csv.NewReader(x.f)

	// reset the row iterator and move until we hit first row with that number of columns
	// assume that that row is the header (TODO: be smarter about this)
	cols, err = x.r.Read()
	for err == nil {
		if len(cols) == bestcols {
			x.head = cols
			return
		}
		cols, err = x.r.Read()
	}
	if err != nil {
		x.stickyErr = err
		return
	}
}

// Next returns the next Record in the document.
// (Implements the formats.Reader interface)
func (x *CSV) Next() (Record, error) {
	cols, err := x.r.Read()
	if err != nil {
		x.stickyErr = err
		return nil, x.stickyErr
	}
	vals := make([][]string, len(cols))
	for i, c := range cols {
		vals[i] = strings.Split(c, csvMultiSplit)
	}

	return &simpleRec{
		fields: x.head,
		values: vals,
	}, nil
}

// Err returns the last error that occured.
func (x *CSV) Err() error {
	return x.stickyErr
}
