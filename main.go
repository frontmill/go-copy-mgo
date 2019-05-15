package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/globalsign/mgo/bson"

	"github.com/globalsign/mgo"
)

var (
	srcDatabaseURL string
	dstDatabaseURL string
)

func init() {
	flag.StringVar(&srcDatabaseURL, "src", "", "source database URL")
	flag.StringVar(&dstDatabaseURL, "dst", "", "destination database URL")
}

type dataBases struct {
	srcSess *mgo.Session
	src     *mgo.Database

	dstSess *mgo.Session
	dst     *mgo.Database
}

func main() {
	flag.Parse()

	dbs := setupDatabase(srcDatabaseURL, dstDatabaseURL)
	defer dbs.close()

	if !dbs.getConfirmation() {
		return
	}

	if err := dbs.copyDatabase(); err != nil {
		log.Fatalln("database copy error: ", err)
	}
	log.Println("database was copied")
}

// setupDatabase establishes a database connection
func setupDatabase(srcURL, dstURL string) *dataBases {
	var dbs dataBases

	if srcURL == "" {
		log.Fatal("source database URL string is empty")
	}

	session, err := mgo.Dial(srcURL)
	if err != nil {
		log.Fatalf("failed to connect to the source database %s\n", srcURL)
	}
	dbs.srcSess = session
	dbs.src = session.DB("")

	if dstURL == "" {
		log.Fatal("destination database URL string is empty")
	}

	session, err = mgo.Dial(dstURL)
	if err != nil {
		log.Fatalf("failed to connect to the destination database %s\n", dstURL)
	}
	dbs.dstSess = session
	dbs.dst = session.DB("")

	return &dbs
}

func (dbs *dataBases) getConfirmation() bool {
	fmt.Printf("database %s will be cleared\nContinue? yes/No ", dbs.dst.Name)

	var clear string
	_, err := fmt.Scanln(&clear)

	if err == nil && clear == "yes" {
		return true
	}
	return false
}

func (dbs *dataBases) close() {
	dbs.srcSess.Close()
	dbs.dstSess.Close()
}

func (dbs *dataBases) copyDatabase() error {
	list, err := dbs.src.CollectionNames()
	if err != nil {
		return err
	}

	for _, collection := range list {
		if strings.Contains(collection, "system") {
			continue
		}

		n, err := dbs.copyCollection(collection)
		if err != nil {
			return err
		}
		documentsCount, err := dbs.src.C(collection).Count()
		if err != nil {
			return err
		}
		log.Printf("collection %s was copied (documents: %d/%d)", collection, n, documentsCount)
	}
	return nil
}

func (dbs *dataBases) copyCollection(collection string) (int, error) {
	log.Printf("dropping collection: %s", collection)
	err := dbs.dst.C(collection).DropCollection()
	err = convertError(err)
	if err != nil && err != mgo.ErrNotFound {
		return 0, fmt.Errorf("collection drop error %s: %s", collection, err.Error())
	}

	log.Printf("setting indexes for a collection: %s", collection)
	listIndex, err := dbs.src.C(collection).Indexes()
	if err != nil {
		return 0, fmt.Errorf("error getting index list: %s", err.Error())
	}

	for _, idx := range listIndex {
		err = dbs.dst.C(collection).EnsureIndex(idx)
		if err != nil {
			return 0, fmt.Errorf("error creating index %s for collection %s: %s", idx.Name, collection, err.Error())
		}
	}

	log.Printf("insert documents into the destination collection: %s", collection)
	query := dbs.src.C(collection).Find(bson.M{})
	iter := query.Iter()
	defer iter.Close()

	var doc interface{}

	i := 0
	for iter.Next(&doc) {
		i++
		err = dbs.dst.C(collection).Insert(doc)
		if err != nil {
			return i, fmt.Errorf("error inserting document into collection %s: %s\n", collection, err.Error())
		}
	}
	return i, nil
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "not found") {
		return mgo.ErrNotFound
	}
	return err
}
