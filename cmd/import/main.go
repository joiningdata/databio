package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joiningdata/databio/sources"
	_ "github.com/mattn/go-sqlite3"
)

func initDB(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE sources (
				source_id integer primary key,
				name varchar,
				description varchar,
				ident_type varchar,
				url varchar,
				id_url varchar,
				citedata varchar
			);`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE UNIQUE INDEX source_names_uqx ON sources (name);`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE source_indexes (
				source_id integer,
				subset varchar,
				bloom blob,
				last_update datetime,
				element_count integer,
				primary key (source_id, subset)
			);`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE source_mappings (
				left_source_id integer, -- NB left_source_id < right_source_id
				right_source_id integer,
				mapfilename varchar,
				map_query_lr varchar,
				map_query_rl varchar,
				last_update datetime,
				element_count integer,
				primary key (left_source_id, right_source_id)
			);`)
	return err
}

func getOrCreateSource(db *sql.DB, sourceName string) (int64, error) {
	var sid int64
	err := db.QueryRow("SELECT source_id FROM sources WHERE name=?;", sourceName).Scan(&sid)
	if err == sql.ErrNoRows {
		res, err2 := db.Exec(`INSERT INTO sources (name) VALUES (?);`, sourceName)
		if err2 == nil {
			return res.LastInsertId()
		}
		err = err2
	}
	return sid, err
}

func createSource(db *sql.DB, sourceName, description, coltype string) error {
	_, err := db.Exec(`INSERT INTO sources (name,description,ident_type)
			VALUES (?,?,?) ON CONFLICT(name) DO UPDATE
			SET description=excluded.description, ident_type=excluded.ident_type;`,
		sourceName, description, coltype)
	return err
}

func updateSourceURLs(db *sql.DB, sourceName, mainURL, idURL string) error {
	if strings.Count(idURL, "%s") != 1 {
		return fmt.Errorf("the ID URL must have a '%%s' placeholder for the identifier")
	}
	_, err := db.Exec(`UPDATE sources SET url=?, id_url=? WHERE name=?;`,
		mainURL, idURL, sourceName)
	return err
}

func createReference(db *sql.DB, sourceName, risFilename string) error {
	citedata, err := ioutil.ReadFile(risFilename)
	if err != nil {
		return err
	}
	_, err = db.Exec(`UPDATE sources SET citedata=? WHERE name=?;`, string(citedata), sourceName)
	return err
}

func loadIndex(db *sql.DB, sourceName, subsetName, filename, updated string) error {
	srcid, err := getOrCreateSource(db, sourceName)
	if err != nil {
		return err
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	originalSize := info.Size()
	items := make(map[string]struct{}, 75000)
	s := bufio.NewScanner(f)
	first := true
	for s.Scan() {
		if first {
			first = false
			continue
		}
		ident := strings.TrimSpace(s.Text())
		if ident == "" {
			continue
		}
		items[ident] = struct{}{}
	}
	f.Close()

	bf := &sources.BloomFilter{}
	bf.Advise(len(items))
	for x := range items {
		bf.Learn(x)
	}

	data := bf.Pack()
	log.Printf("%s[%s] :: %s = %d items indexed (%dkb => %dkb [%d%%])", sourceName, subsetName, filename,
		len(items), originalSize/1024, len(data)/1024, (len(data)*100)/int(originalSize+1))
	_, err = db.Exec(`INSERT INTO source_indexes (source_id,subset,last_update,element_count,bloom)
		VALUES (?,?,?,?,?);`, srcid, subsetName, updated, len(items), data)
	if err != nil {
		log.Println(subsetName)
	}
	return err
}

func createMapping(db *sql.DB, leftSourceName, rightSourceName, filename, updated string) error {
	leftID, err := getOrCreateSource(db, leftSourceName)
	if err != nil {
		return err
	}
	rightID, err := getOrCreateSource(db, rightSourceName)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	fullpath := filepath.Base(filename)

	swapped := 0
	if rightID < leftID {
		swapped = 1
		leftID, rightID = rightID, leftID
	}
	q1 := fmt.Sprintf("SELECT right_id FROM mapping_%d_to_%d WHERE left_id=?;", leftID, rightID)
	q2 := fmt.Sprintf("SELECT left_id FROM mapping_%d_to_%d WHERE right_id=?;", leftID, rightID)
	_, err = tx.Exec(`INSERT INTO source_mappings (left_source_id,right_source_id,mapfilename,last_update,
		map_query_lr,map_query_rl) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING;`,
		leftID, rightID, fullpath, updated, q1, q2)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS mapping_%d_to_%d (
			left_id varchar,
			right_id varchar,
			primary key(left_id,right_id)
		);`, leftID, rightID))
	if err != nil {
		tx.Rollback()
		return err
	}

	stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO mapping_%d_to_%d (left_id,right_id)
		VALUES (?,?) ON CONFLICT DO NOTHING;`, leftID, rightID))
	if err != nil {
		tx.Rollback()
		return err
	}

	/// read in the entire mapping file
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	n := 0
	s := bufio.NewScanner(f)
	s.Scan() // skip header
	for s.Scan() {
		row := strings.Split(s.Text(), "\t")
		left := strings.TrimSpace(row[swapped])
		right := strings.TrimSpace(row[1-swapped])
		if left == "" || right == "" {
			// skip any pairs with a blank
			continue
		}
		_, err = stmt.Exec(left, right)
		if err != nil {
			tx.Rollback()
			return err
		}
		n++
	}
	f.Close()
	log.Printf("%s<>%s :: %s = %d pairs mapped", leftSourceName, rightSourceName, filename, n)
	_, err = tx.Exec(fmt.Sprintf(`UPDATE source_mappings SET element_count=(SELECT COUNT(*) FROM mapping_%d_to_%d)
		WHERE left_source_id=%d AND right_source_id=%d;`, rightID, leftID, leftID, rightID))

	_, err = tx.Exec(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS mapping_%d_to_%d_idx ON mapping_%d_to_%d (right_id,left_id);`,
		rightID, leftID, leftID, rightID))
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func main() {
	envSourceDB, ok := os.LookupEnv("DATABIO_DB")
	if !ok {
		envSourceDB = "sources.sqlite"
	}
	logfilename, ok := os.LookupEnv("DATABIO_LOGS")
	if ok {
		f, err := os.Create(logfilename)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(f)
		defer f.Close()
	}
	log.SetFlags(log.LstdFlags)
	dbfile := flag.String("db", envSourceDB, "sqlite database `filename` for source identifers")
	coltype := flag.String("t", "text", "`type` of the identifers (integers, floats, prefixed integers, text)")
	upDate := flag.String("d", "", "`datetime` for the fetch of the updated data")
	subsetname := flag.String("s", "", "`name` of the subset when indexing (blank=all)")
	flag.Parse()

	db, err := sql.Open("sqlite3", *dbfile)
	if err != nil {
		log.Fatal(err)
	}

	if *upDate == "" {
		// NB the automatic timestamp truncates seconds intentionally,
		// whereas the timestamp in the update scripts includes them.
		*upDate = time.Now().Format("2006-01-02T15:04")
	}

	switch flag.Arg(0) {
	case "init":
		err = initDB(db)

	case "new": // [-t type] reverse.dotted.source.identifier "text description of source"
		err = createSource(db, flag.Arg(1), flag.Arg(2), *coltype)

	case "urls", "url": // reverse.dotted.source.identifier https://source.url https://source.url/ident/%s
		err = updateSourceURLs(db, flag.Arg(1), flag.Arg(2), flag.Arg(3))

	case "refs", "ref": // reverse.dotted.source.identifier reference.ris
		err = createReference(db, flag.Arg(1), flag.Arg(2))

	case "index": // [-s subset] reverse.dotted.source.identifier identifier_filename.txt
		err = loadIndex(db, flag.Arg(1), *subsetname, flag.Arg(2), *upDate)

	case "map": // reverse.dotted.left.source.identifier reverse.dotted.right.source.identifier mapping_filename.tsv
		err = createMapping(db, flag.Arg(1), flag.Arg(2), flag.Arg(3), *upDate)

	default:
		log.Fatal("supported commands: init, new, urls, refs, index, map")
	}
	if err != nil {
		log.Fatal(err)
	}

	db.Close()
}
