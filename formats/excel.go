package formats

import (
	"bytes"
	"io"
	"log"

	"github.com/360EntSecGroup-Skylar/excelize"
)

const (
	// check at most 5000 rows for header content
	xlsxHeaderCheckMaxRows = 5000
)

var (
	_ = Register(&Format{
		Name:        "Excel XLSX",
		Description: "Microsoft Excel 2007+ Spreadsheet",
		Extensions:  []string{".xlsx"},
		MediaTypes:  []string{"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		Detect:      detectXLSX,
		NewReader: func(r io.Reader) (Reader, error) {
			// TODO: this'll panic if necessary, but we could do it cleaner later
			return OpenXLSX(r.(io.ReadSeeker))
		},
		NewWriter: writerNotSupported,
	})
)

func detectXLSX(data []byte, incomplete bool) (supported, more bool) {
	defer func() {
		if e := recover(); e != nil {
			supported = false
			more = false
		}
	}()

	if incomplete {
		hasMagic := (data[0] == 0x50) && (data[1] == 0x4b) && (data[2] == 0x03) && (data[3] == 0x04)
		return hasMagic, true
	}

	_, err := excelize.OpenReader(bytes.NewReader(data))
	return err == nil, false
}

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
func OpenXLSX(in io.Reader) (*XLSX, error) {
	// excelize.OpenReader reads the entire file into memory,
	// so we're done with 'in' when this finishes
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
