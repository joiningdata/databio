package formats

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
)

const (
	// check at most 5000 rows for header content
	xlsxHeaderCheckMaxRows = 5000

	excelMultiSplit = "|"
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
		cols := x.rows.Columns()
		truecols := make([]string, 0, len(cols))
		for i, c := range cols {
			if strings.TrimSpace(c) == "" {
				continue
			}
			for i > len(truecols) {
				truecols = append(truecols, fmt.Sprintf("Column %c", 'A'+len(truecols)))
			}
			truecols = append(truecols, strings.TrimSpace(c))
		}
		cols = truecols

		nr++
		colcounts[len(cols)]++
		if colcounts[len(cols)] > colcounts[bestcols] {
			bestcols = len(cols)
		}
		if nr > xlsxHeaderCheckMaxRows {
			break
		}
	}
	log.Println("BEST COLS ", colcounts)

	// reset the row iterator and move until we hit first row with that number of columns
	// assume that that row is the header (TODO: be smarter about this)
	x.rows, x.stickyErr = x.f.Rows(x.sheetMap[x.currentSheet])
	if x.stickyErr != nil {
		return
	}
	for x.rows.Next() {
		cols := x.rows.Columns()
		truecols := make([]string, 0, len(cols))
		for i, c := range cols {
			if strings.TrimSpace(c) == "" {
				continue
			}
			for i > len(truecols) {
				truecols = append(truecols, fmt.Sprintf("Column %c", 'A'+len(truecols)))
			}
			truecols = append(truecols, strings.TrimSpace(c))
		}
		cols = truecols

		if len(cols) == bestcols {
			x.head = cols
			return
		}
	}
}

// Next returns the next Record in the document.
// (Implements the formats.Reader interface)
func (x *XLSX) Next() (Record, error) {
	if !x.rows.Next() {
		x.stickyErr = x.rows.Error()
		if x.stickyErr == nil {
			x.stickyErr = io.EOF
		}
		return nil, x.stickyErr
	}

	cols := x.rows.Columns()
	if len(cols) > len(x.head) {
		cols = cols[:len(x.head)]
	}

	vals := make([][]string, len(cols))
	for i, c := range cols {
		vals[i] = strings.Split(c, excelMultiSplit)
	}

	return &simpleRec{
		fields: x.head,
		values: vals,
	}, nil
}

// Err returns the last error that occured.
func (x *XLSX) Err() error {
	return x.stickyErr
}
