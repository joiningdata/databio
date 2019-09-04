package sources

import (
	"bufio"
	"database/sql"
	"errors"
	"log"
	"sort"
	"strings"

	"github.com/joiningdata/databio"

	// only support sqlite3
	_ "github.com/mattn/go-sqlite3"
)

const (
	exampleHitSize = 10
)

// A Database of source identifiers and references to mapping resources between them.
type Database struct {
	db *sql.DB

	Sources  map[string]*Source
	mappings map[string]map[string]string
}

// Open a source database and load it into memory.
func Open(filename string) (*Database, error) {
	sdb, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	rows, err := sdb.Query("SELECT source_id,name,description,ident_type,url FROM sources;")
	if err != nil {
		return nil, err
	}

	srcs := make(map[string]*Source)
	for rows.Next() {
		s := &Source{}
		err = rows.Scan(&s.ID, &s.Name, &s.Description, &s.IdentifierType, &s.URL)
		if err != nil {
			rows.Close()
			return nil, err
		}
		srcs[s.Name] = s
		log.Println(s.Name, s)
	}
	rows.Close()

	for _, src := range srcs {
		src.Subsets = make(map[string]*BloomFilter)

		rows, err := sdb.Query("SELECT subset, bloom FROM source_indexes WHERE source_id=?;", src.ID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			ss := ""
			var bfdata []byte
			err = rows.Scan(&ss, &bfdata)
			if err != nil {
				rows.Close()
				return nil, err
			}
			bf := &BloomFilter{}
			err = bf.Unpack(bfdata)
			if err != nil {
				rows.Close()
				return nil, err
			}
			src.Subsets[ss] = bf
		}
		rows.Close()
	}

	rows, err = sdb.Query(`SELECT a.name, b.name, c.mapfilename
		FROM sources a, sources b, source_mappings c
		WHERE a.source_id=c.left_id AND b.source_id=c.right_id;`)
	if err != nil {
		return nil, err
	}

	maps := make(map[string]map[string]string)
	for rows.Next() {
		var left, right, pathname string
		err = rows.Scan(&left, &right, &pathname)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := maps[left]; !ok {
			maps[left] = make(map[string]string)
		}
		maps[left][right] = pathname
		log.Println(left, right, pathname)
	}
	rows.Close()

	db := &Database{
		db:       sdb,
		Sources:  srcs,
		mappings: maps,
	}
	return db, err
}

// SourceHit describes a search hit and some statistics.
type SourceHit struct {
	// SourceName of the database hit.
	SourceName string
	// Subset of the database if defined.
	Subset string
	// Hits is the number of samples that hit the database.
	Hits uint64
	// Tested is the number of sample values tested.
	Tested uint64
	// SubsetRatio indicates the percentage of the subset covered by the sample.
	// E.g. Hits / |Subset|
	SubsetRatio float64 // 0.0 - 1.0
	// SubsetRatio indicates the percentage of the sample covered by the subset.
	// E.g. Hits / |Sample|
	SampleRatio float64 // 0.0 - 1.0
	// ExpectedError rate of hits for the source tested.
	ExpectedError float64 // 0.0-1.0
	// Examples lists some sample values that were in the hit set.
	Examples []string
}

// Mappings returns a list of sources that the named Source can be mapped to.
func (x *Database) Mappings(sourceName string) []string {
	var res []string
	for left, m := range x.mappings {
		for right := range m {
			if left == sourceName {
				res = append(res, right)
			}
			if right == sourceName {
				res = append(res, left)
			}
		}
	}
	return res
}

// GetMapping returns a map from the given source IDs to another source IDs.
// It currently only supports 1-1 mappings.
// TODO: implement 1-M mappings
func (x *Database) GetMapping(fromID, toID string) (map[string]string, error) {
	retCol := -1
	fn := ""

	for left, m := range x.mappings {
		for right, mapfile := range m {
			if left == fromID && right == toID {
				fn = mapfile
				retCol = 1
				break
			}
			if left == toID && right == fromID {
				fn = mapfile
				retCol = 0
				break
			}
		}
		if retCol != -1 {
			break
		}
	}

	if fn == "" {
		return nil, errors.New("databio/sources: no supported mapping")
	}

	res := make(map[string]string, 75000)
	f, err := databio.OpenSourceMap(fn)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)
	s.Scan() // skip header
	for s.Scan() {
		row := strings.Split(s.Text(), "\t")
		res[row[1-retCol]] = row[retCol]
	}
	f.Close()
	return res, nil
}

// DetermineSource examines the sample data given and tries to guess which
// source database it came from. It returns a sorted list of possible
// Sources along with additional statistics.
func (x *Database) DetermineSource(sample []string) []*SourceHit {
	var res []*SourceHit
	for srcName, src := range x.Sources {
		for subsetName, bf := range src.Subsets {
			var hits uint64
			var ex []string
			for _, s := range sample {
				if yes, _ := bf.Detect(s); yes {
					if len(ex) < exampleHitSize {
						ex = append(ex, s)
					}
					hits++
				}
			}
			if hits == 0 {
				continue
			}

			res = append(res, &SourceHit{
				SourceName:    srcName,
				Subset:        subsetName,
				Hits:          hits,
				Tested:        uint64(len(sample)),
				SubsetRatio:   float64(hits) / float64(bf.Count()),
				SampleRatio:   float64(hits) / float64(len(sample)),
				ExpectedError: bf.EstimatedErrorRate(),
				Examples:      ex,
			})
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Hits == res[j].Hits {
			return res[i].SubsetRatio > res[j].SubsetRatio
		}
		return res[i].Hits > res[j].Hits
	})
	return res
}

// A Source of identifiers.
type Source struct {
	ID             int64
	Name           string
	Description    string
	IdentifierType string
	URL            string

	Subsets map[string]*BloomFilter
}
