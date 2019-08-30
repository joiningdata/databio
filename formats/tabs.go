package formats

import (
	"bufio"
	"io"
	"os"
	"strings"
)

const (
	// check at most 5000 rows for header content
	tsvHeaderCheckMaxRows = 5000
)

// TSV supports reading tabular records from a TSV file.
type TSV struct {
	f *os.File
	s *bufio.Scanner

	head []string

	stickyErr error
}

// OpenTSV opens a TSV document and returns a formats.Reader.
func OpenTSV(in *os.File) (*TSV, error) {
	r := bufio.NewScanner(in)

	x := &TSV{
		f: in,
		s: r,
	}

	x.skipHeaders()

	return x, x.stickyErr
}

func (x *TSV) skipHeaders() {
	// if there are descriptive lines etc at the top we try to skip over them

	// out of the first N rows, which number of columns is the most frequent?
	bestcols := 0
	nr := 0
	colcounts := make(map[int]int)
	for x.s.Scan() {
		cols := strings.Split(x.s.Text(), "\t")
		nr++
		colcounts[len(cols)]++
		if colcounts[len(cols)] > colcounts[bestcols] {
			bestcols = len(cols)
		}
		if nr > tsvHeaderCheckMaxRows {
			break
		}
	}
	x.stickyErr = x.s.Err()
	if x.stickyErr != nil {
		return
	}

	// TODO: verify that it actually looks like a TSV

	_, x.stickyErr = x.f.Seek(0, io.SeekStart)
	if x.stickyErr != nil {
		return
	}
	x.s = bufio.NewScanner(x.f)

	// reset the row iterator and move until we hit first row with that number of columns
	// assume that that row is the header (TODO: be smarter about this)
	for x.s.Scan() {
		cols := strings.Split(x.s.Text(), "\t")
		if len(cols) == bestcols {
			x.head = cols
			return
		}
	}
	x.stickyErr = x.s.Err()
}

// Next returns the next Record in the document.
// (Implements the formats.Reader interface)
func (x *TSV) Next() (*Record, error) {
	if !x.s.Scan() {
		x.stickyErr = x.s.Err()
		if x.stickyErr == nil {
			x.stickyErr = io.EOF
		}
		return nil, x.stickyErr
	}
	cols := strings.Split(x.s.Text(), "\t")

	return &Record{
		Fields: x.head,
		Values: cols,
	}, nil
}

// Err returns the last error that occured.
func (x *TSV) Err() error {
	return x.stickyErr
}
