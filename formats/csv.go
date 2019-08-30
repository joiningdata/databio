package formats

import (
	"encoding/csv"
	"io"
	"os"
)

const (
	// check at most 5000 rows for header content
	csvHeaderCheckMaxRows = 5000
)

// CSV supports reading tabular records from an csv file.
type CSV struct {
	f *os.File
	r *csv.Reader

	head []string

	stickyErr error
}

// OpenCSV opens a csv document and returns a formats.Reader.
func OpenCSV(in *os.File) (*CSV, error) {
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
func (x *CSV) Next() (*Record, error) {
	cols, err := x.r.Read()
	if err != nil {
		x.stickyErr = err
		return nil, x.stickyErr
	}

	return &Record{
		Fields: x.head,
		Values: cols,
	}, nil
}

// Err returns the last error that occured.
func (x *CSV) Err() error {
	return x.stickyErr
}
