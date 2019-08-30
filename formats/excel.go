package formats

import (
	"io"
	"log"
	"os"

	"github.com/360EntSecGroup-Skylar/excelize"
)

const (
	// check at most 5000 rows for header content
	xlsxHeaderCheckMaxRows = 5000
)

// XLSX supports reading tabular records from an excel file.
type XLSX struct {
	f *excelize.File

	currentSheet int
	sheetMap     map[int]string

	head []string
	rows *excelize.Rows

	stickyErr error
}

// OpenXLSX opens an excel document and returns a formats.Reader.
func OpenXLSX(in *os.File) (*XLSX, error) {
	f, err := excelize.OpenReader(in)
	if err != nil {
		return nil, ErrUnsupportedFormat
	}

	x := &XLSX{
		f:            f,
		currentSheet: 1,
		sheetMap:     f.GetSheetMap(),
	}

	log.Printf("+%v", x.sheetMap)

	x.skipHeaders()

	return x, x.stickyErr
}

// NextSheet moves to the next Sheet in an excel document.
func (x *XLSX) NextSheet() error {
	x.currentSheet++
	if _, ok := x.sheetMap[x.currentSheet]; !ok {
		return io.EOF
	}
	x.head = nil
	x.skipHeaders()
	return x.stickyErr
}

func (x *XLSX) skipHeaders() {
	// if there are descriptive lines etc at the top we try to skip over them
	x.rows, x.stickyErr = x.f.Rows(x.sheetMap[x.currentSheet])
	if x.stickyErr != nil {
		return
	}

	// out of the first N rows, which number of columns is the most frequent?
	bestcols := 0
	nr := 0
	colcounts := make(map[int]int)
	for x.rows.Next() {
		cols, err := x.rows.Columns()
		if err != nil {
			x.stickyErr = err
			return
		}
		nr++
		colcounts[len(cols)]++
		if colcounts[len(cols)] > colcounts[bestcols] {
			bestcols = len(cols)
		}
		if nr > xlsxHeaderCheckMaxRows {
			break
		}
	}

	// reset the row iterator and move until we hit first row with that number of columns
	// assume that that row is the header (TODO: be smarter about this)
	x.rows, x.stickyErr = x.f.Rows(x.sheetMap[x.currentSheet])
	if x.stickyErr != nil {
		return
	}
	for x.rows.Next() {
		cols, err := x.rows.Columns()
		if err != nil {
			x.stickyErr = err
			return
		}
		if len(cols) == bestcols {
			x.head = cols
			return
		}
	}
}

// Next returns the next Record in the document.
// (Implements the formats.Reader interface)
func (x *XLSX) Next() (*Record, error) {
	if !x.rows.Next() {
		x.stickyErr = x.rows.Error()
		return nil, x.stickyErr
	}

	cols, err := x.rows.Columns()
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
func (x *XLSX) Err() error {
	return x.stickyErr
}
