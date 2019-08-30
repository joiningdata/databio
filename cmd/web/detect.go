package main

import (
	"encoding/json"
	"log"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/joiningdata/databio/formats"
)

const maxSamples = 5000

var (
	detectionStarted int32
	detectionChan    = make(chan detectInfo, 4)
)

type detectInfo struct {
	inputFilename     string
	detectionFilename string
}

func startDetection(fname, dk string) {
	if atomic.LoadInt32(&detectionStarted) == 0 {
		atomic.StoreInt32(&detectionStarted, 1)
		go runDetection()
	}

	d := detectInfo{
		inputFilename:     fname,
		detectionFilename: dk,
	}
	detectionChan <- d
}

func runDetection() {
	for di := range detectionChan {
		f, err := os.Open(di.inputFilename)
		if err != nil {
			log.Println("stage0", di, err)
			writeMsg(di.detectionFilename, "error", "unable to read input")
			continue
		}

		r, err := formats.Open(f)
		if err != nil {
			log.Println("stage1", di, err)
			writeMsg(di.detectionFilename, "error", "unable to parse input")
			continue
		}

		///// collect a sample of the input records
		samples := make(map[string][]string)
		n := 0
		rec, err := r.Next()
		for err == nil {
			n++
			if n > maxSamples {
				break
			}
			for i, colname := range rec.Fields {
				samples[colname] = append(samples[colname], rec.Values[i])
			}

			rec, err = r.Next()
		}

		///// determine if each column is numeric or text
		coltypes := make(map[string]string)
		pfxint := regexp.MustCompile("^[A-Za-z]*:[0-9]*$")
		for colname, sample := range samples {
			nIntegers := 0
			nFloats := 0
			nPrefixedIntegers := 0

			for _, s := range sample {
				_, err := strconv.ParseInt(s, 10, 64)
				if err == nil {
					nIntegers++
					nFloats++
					continue
				}
				_, err = strconv.ParseFloat(s, 64)
				if err == nil {
					nFloats++
					continue
				}

				///////////
				if pfxint.MatchString(s) {
					nPrefixedIntegers++
				}
			}

			if nFloats > nIntegers && nFloats > nPrefixedIntegers {
				coltypes[colname] = "floats"
			} else if nIntegers > nPrefixedIntegers {
				coltypes[colname] = "integers"
			} else if nPrefixedIntegers >= len(sample)/2 {
				coltypes[colname] = "prefixed integers"
			} else {
				coltypes[colname] = "text"
			}
		}

		////////////
		// try to classify each column's source
		colsrcs := make(map[string]map[string]float64)
		sourcemaps := make(map[string][]string)
		for colname, ctype := range coltypes {
			sample := samples[colname]
			sources := identify(ctype, sample)
			colsrcs[colname] = sources

			for s := range sources {
				if _, ok := sourcemaps[s]; ok {
					continue
				}
				sourcemaps[s] = getMappings(s)
			}
		}

		writeMsg(di.detectionFilename, map[string]interface{}{
			"types": coltypes, "sources": colsrcs, "maps": sourcemaps,
		})
	}
}

func writeMsg(fn string, kvmsg ...interface{}) error {
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	if len(kvmsg) == 1 {
		err = json.NewEncoder(f).Encode(kvmsg[0])
	} else {
		m := make(map[interface{}]interface{})
		for i := 0; i < len(kvmsg); i += 2 {
			m[kvmsg[i]] = kvmsg[i+1]
		}
		err = json.NewEncoder(f).Encode(m)
	}
	f.Close()
	return err
}
