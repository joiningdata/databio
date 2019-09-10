package detection

import (
	"strings"

	"github.com/joiningdata/databio/sources"
)

func (d *Detector) identify(coltype string, data []string) map[string]*sources.SourceHit {
	srchits := d.src.DetermineSource(data)
	res := make(map[string]*sources.SourceHit)
	threshold := 0.0
	for i, sh := range srchits {
		if i == 0 {
			threshold = sh.SubsetRatio / 2.0
		} else {
			if sh.SubsetRatio < threshold {
				break
			}
		}
		if sh.SampleRatio < 0.05 {
			continue
		}

		if sh.SampleRatio > sh.ExpectedError {
			if old, found := res[sh.SourceName]; !found || old.SampleRatio < sh.SampleRatio {
				res[sh.SourceName] = sh
			}
		}
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
