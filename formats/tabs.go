package formats

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

const (
	// check at most 5000 rows for header content
	tsvHeaderCheckMaxRows = 5000

	tsvMultiSplit = "|"
)

var (
	_ = Register(&Format{
		Name:        "TSV",
		Description: "Tab-delimited Values",
		Extensions:  []string{".tsv", ".txt", ".tab"},
		MediaTypes:  []string{"text/tab-separated-values"},
		Detect:      detectTSV,
		NewReader: func(r io.Reader) (Reader, error) {
			// TODO: this'll panic if necessary, but we could do it cleaner later
			return OpenTSV(r.(io.ReadSeeker))
		},
		NewWriter: writerNotSupported,
	})
)

func detectTSV(data []byte, incomplete bool) (supported, more bool) {
	defer func() {
		if e := recover(); e != nil {
			supported = false
			more = false
		}
	}()

	if incomplete {
		idx := bytes.LastIndexByte(data, '\n')
		if idx == -1 {
			// not even a full line, need more data
			return false, true
		}
		data = data[:idx]
	}
	ntabs := 0
	nlines := 0
	tabcounts := make(map[int]int)
	for _, line := range bytes.Split(data, []byte("\n")) {
		ntabs = bytes.Count(line, []byte("\t"))
		tabcounts[ntabs]++
		nlines++
	}

	if ntabs > 0 && tabcounts[ntabs] >= 2 {
		// we got 2 or more lines of the same number of columns as the last line
		// so it's probably TSV, more data likely won't help
		return true, false
	}

	if nlines <= 10 {
		// not enough lines to know yet, more data might help
		return false, true
	}

	// we got more than 10 lines, so it's at least text...
	// but its all single-column so probably not the best fit
	return true, false
}

// TSV supports reading tabular records from a TSV file.
type TSV struct {
	f io.ReadSeeker
	s *bufio.Scanner

	head []string

	stickyErr error
}

// OpenTSV opens a TSV document and returns a formats.Reader.
// An io.ReadSeeker is required due to header detection readahead.
func OpenTSV(in io.ReadSeeker) (*TSV, error) {
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

	// TODO: autodetect per-column multi-value split delimiters
	// tsvMultiSplit

	x.stickyErr = x.s.Err()
}

// Next returns the next Record in the document.
// (Implements the formats.Reader interface)
func (x *TSV) Next() (Record, error) {
	if !x.s.Scan() {
		x.stickyErr = x.s.Err()
		if x.stickyErr == nil {
			x.stickyErr = io.EOF
		}
		return nil, x.stickyErr
	}
	cols := strings.Split(x.s.Text(), "\t")
	vals := make([][]string, len(cols))
	for i, c := range cols {
		vals[i] = strings.Split(c, tsvMultiSplit)
	}

	return &simpleRec{
		fields: x.head,
		values: vals,
	}, nil
}

// Err returns the last error that occured.
func (x *TSV) Err() error {
	return x.stickyErr
}
