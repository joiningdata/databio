package main

import (
	"strings"

	"github.com/joiningdata/databio/sources"
)

var (
	srcDB *sources.Database
)

func identify(coltype string, data []string) map[string]float64 {
	srchits := srcDB.DetermineSource(data)
	res := make(map[string]float64)
	threshold := 0.0
	for i, sh := range srchits {
		if i == 0 {
			threshold = sh.Ratio / 2.0
		} else {
			if sh.Ratio < threshold {
				break
			}
		}
		res[sh.SourceName] = sh.Ratio * 100.0
	}
	return res
}

func identifyIntegers(data []string) map[string]float64 {
	return nil
}

func identifyPrefixedIntegers(data []string) map[string]float64 {
	prefixes := make(map[string]float64)
	for _, d := range data {
		parts := strings.Split(d, ":")
		prefixes[parts[0]] += 1.0 / float64(len(data))
	}
	// TODO: compare the prefixes to known dbxrefs
	return prefixes
}

func identifyText(data []string) map[string]float64 {
	wordsoup := make(map[string]float64)
	for _, s := range data {
		words := strings.Split(s, " ")
		for _, w := range words {
			wordsoup[w]++
		}
	}
	for w, n := range wordsoup {
		wordsoup[w] = n / float64(len(wordsoup))
	}
	// TODO: compare the word vector to known sources
	return wordsoup
}
