package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/joiningdata/databio/formats"
)

func getMappings(src string) []string {
	return srcDB.Mappings(src)
}

var (
	translationStarted int32
	translationChan    = make(chan translateInfo, 4)
)

type translateInfo struct {
	inputFilename       string
	translationFilename string

	fromField string
	fromID    string
	toID      string
}

func startTranslation(fname, dk, fromField, fromID, toID string) {
	if atomic.LoadInt32(&translationStarted) == 0 {
		atomic.StoreInt32(&translationStarted, 1)
		go runTranslation()
	}

	d := translateInfo{
		inputFilename:       fname,
		translationFilename: dk,
		fromField:           fromField,
		fromID:              fromID,
		toID:                toID,
	}
	translationChan <- d
}

func runTranslation() {
	for di := range translationChan {
		ext := filepath.Ext(di.inputFilename)
		outputFilename := strings.Replace(di.inputFilename, ext, ".translated.csv", 1)

		translator, err := srcDB.GetTranslator(di.fromID, di.toID)
		if err != nil {
			log.Println("stage0", di, err)
			writeMsg(di.translationFilename, "error", "unable to get translator")
			continue
		}

		f, err := os.Open(di.inputFilename)
		if err != nil {
			log.Println("stage1", di, err)
			writeMsg(di.translationFilename, "error", "unable to read input")
			continue
		}

		r, err := formats.Open(f)
		if err != nil {
			log.Println("stage2", di, err)
			writeMsg(di.translationFilename, "error", "unable to parse input")
			continue
		}

		fout, err := os.Create(outputFilename)
		if err != nil {
			log.Println("stage3", di, err)
			writeMsg(di.translationFilename, "error", "unable to create output")
			continue
		}
		defer fout.Close()
		csvwr := csv.NewWriter(fout)
		defer csvwr.Flush()

		numMissing := 0
		first := true
		rec, err := r.Next()
		for err == nil {
			missing := false
			row := rec.Map()
			vals, ok := row[di.fromField]
			if ok {
				for i, v := range vals {
					vals[i], ok = translator[v]
					if !ok {
						missing = true
					}
				}
				row[di.fromField] = vals
			}

			if missing {
				log.Println("missing data", rec)
				numMissing++

				rec, err = r.Next()
				continue
			}

			if first {
				first = false
				err = csvwr.Write(rec.Fields)
				if err != nil {
					log.Println("stage4", di, err)
					writeMsg(di.translationFilename, "error", "unable to write header to output")
					return
				}
			}

			line := make([]string, len(rec.Fields))
			for i, v := range rec.Fields {
				line[i] = strings.Join(row[v], "|")
			}
			err = csvwr.Write(line)
			if err != nil {
				log.Println("stage4", di, err)
				writeMsg(di.translationFilename, "error", "unable to write to output")
				return
			}

			rec, err = r.Next()
		}
		if err != io.EOF {
			log.Println("stage99", di, err)
			writeMsg(di.translationFilename, "error", "unable to translate")
			return
		}

		///////////////

		logText := "this is the timestamped and hashed logs for reproduction"
		methodsText := "this is the methods text"
		citations := []string{"this is the citations ready for import to endnote/zotero", "this is citation #2"}
		writeMsg(di.translationFilename, map[string]interface{}{
			"log":            logText,
			"methods":        methodsText,
			"citations":      citations,
			"newfilename":    filepath.Base(outputFilename),
			"_localfilename": outputFilename,
		})
	}
}
