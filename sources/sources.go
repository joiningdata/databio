package sources

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	// only support sqlite3
	_ "github.com/mattn/go-sqlite3"
)

const (
	exampleHitSize   = 10
	defaultCacheSize = 16 * 1024
)

// A Database of source identifiers and references to mapping resources between them.
type Database struct {
	db *sql.DB

	Sources map[string]*Source

	mappings map[string]map[string]string
	mappers  map[string]*dbMapper
}

// Mapper represents a one-way mapping between identifier sources.
type Mapper interface {
	// Get retrieves ids that map to the given id.
	Get(leftID string) (rightIDs []string, found bool)
}

type dbMapper struct {
	stmt  *sql.Stmt
	mu    sync.RWMutex
	cache *Cache
}

func (m *dbMapper) Close() {
	m.cache.Clear()
	m.stmt.Close()
}

func (m *dbMapper) Get(leftID string) (rightIDs []string, found bool) {
	m.mu.RLock()
	if r, ok := m.cache.Get(leftID); ok {
		m.mu.RUnlock()
		return r, ok
	}
	m.mu.RUnlock()
	rows, err := m.stmt.Query(leftID)
	if err == sql.ErrNoRows {
		m.mu.Lock()
		m.cache.Add(leftID, []string{})
		m.mu.Unlock()
		return
	}
	if err != nil {
		log.Println(err)
		return
	}
	found = true
	for rows.Next() {
		var r string
		err = rows.Scan(&r)
		if err != nil {
			log.Println(err)
			rows.Close()
			return
		}
		rightIDs = append(rightIDs, r)
	}
	rows.Close()
	m.mu.Lock()
	m.cache.Add(leftID, rightIDs)
	m.mu.Unlock()
	return
}

// Open a source database and load it into memory.
func Open(filename string) (*Database, error) {
	sdb, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	rows, err := sdb.Query("SELECT source_id,name,description,ident_type,url,id_url,citedata FROM sources;")
	if err != nil {
		return nil, err
	}

	srcs := make(map[string]*Source)
	for rows.Next() {
		s := &Source{}
		err = rows.Scan(&s.ID, &s.Name, &s.Description, &s.IdentifierType, &s.URL, &s.LinkoutURL, &s.Citation)
		if err != nil {
			rows.Close()
			return nil, err
		}
		srcs[s.Name] = s
	}
	rows.Close()

	for _, src := range srcs {
		src.Subsets = make(map[string]*BloomFilter)

		rows, err := sdb.Query("SELECT subset, last_update, bloom FROM source_indexes WHERE source_id=?;", src.ID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			ss := ""
			var bfdata []byte
			var tm time.Time
			err = rows.Scan(&ss, &tm, &bfdata)
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
			src.LastUpdate = tm
		}
		rows.Close()
	}

	rows, err = sdb.Query(`SELECT a.name, b.name, c.map_query_lr, c.map_query_rl
		FROM sources a, sources b, source_mappings c
		WHERE a.source_id=c.left_source_id AND b.source_id=c.right_source_id;`)
	if err != nil {
		return nil, err
	}

	maps := make(map[string]map[string]string)
	for rows.Next() {
		var left, right, q1, q2 string
		err = rows.Scan(&left, &right, &q1, &q2)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := maps[left]; !ok {
			maps[left] = make(map[string]string)
		}
		if _, ok := maps[right]; !ok {
			maps[right] = make(map[string]string)
		}
		maps[left][right] = q1
		maps[right][left] = q2
	}
	rows.Close()

	db := &Database{
		db:       sdb,
		Sources:  srcs,
		mappings: maps,
		mappers:  make(map[string]*dbMapper),
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
	// UniqueHits is the number of sample values that hit the database.
	UniqueHits uint64
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
	for right := range x.mappings[sourceName] {
		res = append(res, right)
	}
	return res
}

// GetMapper returns a mapper from the given source IDs to another source IDs.
func (x *Database) GetMapper(fromID, toID string) (Mapper, error) {
	f1, ok := x.mappings[fromID]
	if !ok {
		return nil, errors.New("databio/sources: no supported mapping")
	}
	q, ok := f1[toID]
	if !ok {
		return nil, errors.New("databio/sources: no supported mapping")
	}

	m, ok := x.mappers[q]
	if ok {
		return m, nil
	}

	stmt, err := x.db.Prepare(q)
	if err != nil {
		return nil, err
	}
	m = &dbMapper{
		stmt:  stmt,
		mu:    sync.RWMutex{},
		cache: NewCache(defaultCacheSize),
	}
	x.mappers[q] = m
	return m, nil
}

// DetermineSource examines the sample data given and tries to guess which
// source database it came from. It returns a sorted list of possible
// Sources along with additional statistics.
func (x *Database) DetermineSource(sample []string) []*SourceHit {
	var res []*SourceHit
	for srcName, src := range x.Sources {
		for subsetName, bf := range src.Subsets {
			uhits := make(map[string]struct{})
			var hits uint64
			var ex []string
			for _, s := range sample {
				if yes, _ := bf.Detect(s); yes {
					if len(ex) < exampleHitSize {
						ex = append(ex, s)
					}
					hits++
					uhits[s] = struct{}{}
				}
			}
			if hits == 0 {
				continue
			}

			res = append(res, &SourceHit{
				SourceName:    srcName,
				Subset:        subsetName,
				Hits:          hits,
				UniqueHits:    uint64(len(uhits)),
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
	LinkoutURL     string
	Citation       string

	Subsets    map[string]*BloomFilter
	LastUpdate time.Time
}

// Linkout directly to an identifier if supported.
func (s *Source) Linkout(toID string) string {
	if s.LinkoutURL != "" {
		if strings.Contains(s.LinkoutURL, "%s") {
			return fmt.Sprintf(s.LinkoutURL, toID)
		}
		return s.LinkoutURL
	}
	return s.URL
}

// Cite extracts and formats a simple citation using the refman-format data.
func (s *Source) Cite() string {
	lines := strings.Split(s.Citation, "\r\n")
	d := make(map[string][]string)
	for _, line := range lines {
		p := strings.SplitN(line, "  - ", 2)
		if len(p) != 2 {
			continue
		}
		key := strings.TrimSpace(p[0])
		val := strings.TrimSpace(p[1])
		d[key] = append(d[key], val)
	}

	author := ""
	title := ""
	journal := ""
	year := ""
	if v, ok := d["AU"]; ok {
		author = strings.SplitN(v[0], ",", 2)[0]
	}
	if v, ok := d["TI"]; ok {
		title = v[0]
	}
	if v, ok := d["T2"]; ok {
		journal = v[0]
	}
	if v, ok := d["PY"]; ok {
		year = v[0]
	}

	return fmt.Sprintf(`%s et al. "%s" %s (%s).`, author, title, journal, year)
}
