package sources

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"sort"
	"strings"

	// only support sqlite3
	_ "github.com/mattn/go-sqlite3"
)

// A Database of source identifiers and references to mapping resources between them.
type Database struct {
	db *sql.DB

	sources  map[string]*Source
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
		sources:  srcs,
		mappings: maps,
	}
	return db, err
}

type SourceHit struct {
	SourceName string
	Subset     string
	Hits       uint64
	Ratio      float64 // 0.0 - 1.0
}

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

func (x *Database) GetTranslator(fromID, toID string) (map[string]string, error) {
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

	res := make(map[string]string, 75000)
	f, err := os.Open(fn)
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

func (x *Database) DetermineSource(sample []string) []*SourceHit {
	var res []*SourceHit
	for srcName, src := range x.sources {
		for subsetName, bf := range src.Subsets {
			var hits uint64
			for _, s := range sample {
				if yes, _ := bf.Detect(s); yes {
					hits++
				}
			}
			if hits == 0 {
				continue
			}
			ratio := float64(hits) / float64(bf.Count())
			res = append(res, &SourceHit{
				SourceName: srcName,
				Subset:     subsetName,
				Hits:       hits,
				Ratio:      ratio,
			})
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Hits == res[j].Hits {
			return res[i].Ratio > res[j].Ratio
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
