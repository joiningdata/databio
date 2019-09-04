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
	_, err = db.Exec(`CREATE TABLE source_indexes (
				source_id integer,
				subset varchar,
				bloom blob,
				primary key (source_id, subset)
			);`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE source_mappings (
				left_id integer, -- NB left_id < right_id
				right_id integer,
				mapfilename varchar,
				primary key (left_id, right_id)
			);`)
	return err
}

func createSource(db *sql.DB, sourceName, description, coltype string) error {
	_, err := db.Exec(`INSERT INTO sources (name,description,ident_type)
			VALUES (?,?,?);`, sourceName, description, coltype)
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

func loadIndex(db *sql.DB, sourceName, subsetName, filename string) error {
	srcid := 0
	err := db.QueryRow("SELECT source_id FROM sources WHERE name=?;", sourceName).Scan(&srcid)
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
	log.Printf("%d items indexed (%dkb => %dkb [%d%%])", len(items), originalSize/1024, len(data)/1024, len(data)/int(originalSize/100))
	_, err = db.Exec("INSERT INTO source_indexes (source_id,subset,bloom) VALUES (?,?,?);", srcid, subsetName, data)
	return err
}

func createMapping(db *sql.DB, leftSourceName, rightSourceName, filename string) error {
	leftID, rightID := 0, 0
	err := db.QueryRow("SELECT source_id FROM sources WHERE name=?;", leftSourceName).Scan(&leftID)
	if err != nil {
		return err
	}
	err = db.QueryRow("SELECT source_id FROM sources WHERE name=?;", rightSourceName).Scan(&rightID)
	if err != nil {
		return err
	}
	fullpath, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO source_mappings (left_id,right_id,mapfilename) VALUES (?,?,?);", leftID, rightID, fullpath)
	return err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dbfile := flag.String("db", "sources.sqlite", "sqlite database `filename` for source identifers")
	coltype := flag.String("t", "text", "`type` of the identifers (integers, floats, prefixed integers, text)")
	subsetname := flag.String("s", "", "`name` of the subset when indexing")
	flag.Parse()

	db, err := sql.Open("sqlite3", *dbfile)
	if err != nil {
		log.Fatal(err)
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
		log.Println(flag.Arg(1), *subsetname, flag.Arg(2))
		err = loadIndex(db, flag.Arg(1), *subsetname, flag.Arg(2))

	case "map": // reverse.dotted.left.source.identifier reverse.dotted.right.source.identifier mapping_filename.tsv
		err = createMapping(db, flag.Arg(1), flag.Arg(2), flag.Arg(3))

	default:
		log.Fatal("supported commands: init, new, index, map")
	}
	if err != nil {
		log.Fatal(err)
	}

	db.Close()
}
