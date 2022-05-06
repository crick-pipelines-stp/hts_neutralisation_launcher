// send slack alert on celebration-worthy milestones

package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

const DB_PATH string = "/home/warchas/.neut_milestones.db"

var milestones = map[string]int{
	"well":     5_000_000,
	"image":    5_000_000,
	"plate":    5_000,
	"workflow": 1_000,
}

type MilestoneDB struct {
	con *sql.DB
}

type LimsDB struct {
	con *sql.DB
}

func handleError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func connectLIMS() LimsDB {
	user := os.Getenv("NE_USER")
	passwd := os.Getenv("NE_PASSWORD")
	host := os.Getenv("NE_HOST_PROD")
	database := "serology"
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, passwd, host, database)
	db, err := sql.Open("mysql", dsn)
	handleError(err)
	return LimsDB{db}
}

func connectMilestone(create bool) MilestoneDB {
	var db MilestoneDB
	if create {
		log.Printf("creating new database at '%s'\n", DB_PATH)
		db = newDB()
	} else {
		log.Printf("using existing database '%s'\n", DB_PATH)
		db = getExistingDB()
	}
	return db
}

func newDB() MilestoneDB {
	const createStr string = `
		CREATE TABLE milestones (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name STRING NOT NULL,
			interval INTEGER NOT NULL DEFAULT 1,
			milestone INTEGER NOT NULL
		)
	`
	db, err := sql.Open("sqlite3", DB_PATH)
	handleError(err)
	_, err = db.Exec(createStr)
	handleError(err)
	querystr := "INSERT INTO milestones (name, milestone) VALUES (?, ?)"
	stmt, err := db.Prepare(querystr)
	handleError(err)
	for name, value := range milestones {
		_, err := stmt.Exec(name, value)
		handleError(err)
	}
	return MilestoneDB{db}
}

func getExistingDB() MilestoneDB {
	// check if database actually exist
	if _, err := os.Stat(DB_PATH); errors.Is(err, os.ErrNotExist) {
		log.Fatalf("ERROR: '%s' does not exist, have you created the db?\n", DB_PATH)
	}
	db, err := sql.Open("sqlite3", DB_PATH)
	handleError(err)
	return MilestoneDB{db}
}

func (db MilestoneDB) isMilestone(name string, n int) (bool, int) {
	var interval, milestone int
	stmt, err := db.con.Prepare(`
		SELECT interval, milestone
		FROM milestones
		WHERE name = ?
	`)
	handleError(err)
	stmt.QueryRow(name).Scan(&interval, &milestone)
	currMilestone := milestone * interval
	isMilestone := n > currMilestone
	return isMilestone, currMilestone
}

func (db MilestoneDB) updateMilestone(name string) {
	stmt, err := db.con.Prepare(`
		UPDATE milestones
		SET interval = interval + 1
		WHERE name = ?
	`)
	handleError(err)
	_, err = stmt.Exec(name)
	handleError(err)
	var milestone, interval int
	stmt2, _ := db.con.Prepare("SELECT milestone, interval FROM milestones WHERE name = ?")
	stmt2.QueryRow(name).Scan(&milestone, &interval)
	log.Printf("Updated %s milestone to %d\n", name, milestone*interval)
}

func (db LimsDB) getMaxWorkflow() int {
	// get greatest workflowID number in workflow tracking table
	var workflow int
	stmt := db.con.QueryRow("SELECT MAX(workflow_id) FROM NE_final_results")
	err := stmt.Scan(&workflow)
	handleError(err)
	log.Printf("current n workflows = %d\n", workflow)
	return workflow
}

func (db LimsDB) getNPlates() int {
	// total number of assay plates
	// use number of stitched plates
	var plates int
	stmt := db.con.QueryRow("SELECT COUNT(*) FROM NE_task_tracking_stitching")
	err := stmt.Scan(&plates)
	handleError(err)
	log.Printf("current n plates = %d\n", plates)
	return plates
}

func (db LimsDB) getNWells() int {
	// total number of wells, including empty, controls etc
	var imgs int
	stmt := db.con.QueryRow("SELECT COUNT(*) FROM NE_raw_index WHERE workflow_id >= 68")
	err := stmt.Scan(&imgs)
	handleError(err)
	wells := imgs / 2 // div2 as there are 2 images per well (channels)
	log.Printf("current n wells = %d\n", wells)
	return wells
}

func (db LimsDB) getNImages() int {
	// total number of images
	var imgs int
	stmt := db.con.QueryRow("SELECT COUNT(*) FROM NE_raw_index WHERE workflow_id >=68")
	err := stmt.Scan(&imgs)
	handleError(err)
	log.Printf("current n images = %d\n", imgs)
	return imgs
}

func checkWorkflows(limsDB LimsDB, mDB MilestoneDB) {
	currWorkflow := limsDB.getMaxWorkflow()
	isMilestone, currMilestone := mDB.isMilestone("workflow", currWorkflow)
	if isMilestone {
		// send slack alert
		msg := fmt.Sprintf(
			":partying_face: New milestone! Over %d workflows (%d) registered",
			currMilestone,
			currWorkflow,
		)
		log.Println(msg)
		err := sendSlackAlert(msg)
		handleError(err)
		mDB.updateMilestone("workflow")
	}
}

func checkWells(limsDB LimsDB, mDB MilestoneDB) {
	currWells := limsDB.getNWells()
	isMilestone, currMilestone := mDB.isMilestone("well", currWells)
	if isMilestone {
		// send slack alert
		msg := fmt.Sprintf(
			":partying_face: New milestone! Over %d wells (%d) assayed",
			currMilestone,
			currWells,
		)
		log.Println(msg)
		err := sendSlackAlert(msg)
		handleError(err)
		mDB.updateMilestone("well")
	}
}

func checkPlates(limsDB LimsDB, mDB MilestoneDB) {
	currPlates := limsDB.getNPlates()
	isMilestone, currMilestone := mDB.isMilestone("plate", currPlates)
	if isMilestone {
		// send slack alert
		msg := fmt.Sprintf(
			":partying_face: New milestone! Over %d wells (%d) assayed",
			currMilestone,
			currPlates,
		)
		log.Println(msg)
		err := sendSlackAlert(msg)
		handleError(err)
		mDB.updateMilestone("plate")
	}
}

func checkImages(limsDB LimsDB, mDB MilestoneDB) {
	currImages := limsDB.getNImages()
	isMilestone, currMilestone := mDB.isMilestone("image", currImages)
	if isMilestone {
		// send slack alert
		msg := fmt.Sprintf(
			":partying_face: New milestone! Over %d images (%d) imaged",
			currMilestone,
			currImages,
		)
		log.Println(msg)
		err := sendSlackAlert(msg)
		handleError(err)
		mDB.updateMilestone("image")
	}
}

func sendSlackAlert(msg string) error {
	webhookURL := os.Getenv("SLACK_WEBHOOK_NEUTRALISATION")
	body := fmt.Sprintf("{'text': '%s'}", msg)
	resp, err := http.Post(
		webhookURL,
		"application/json",
		bytes.NewBufferString(body),
	)
	handleError(err)
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		return errors.New(buf.String())
	}
	return nil
}

func main() {
	create := os.Getenv("CREATE") == "1"
	mDB := connectMilestone(create)
	limsDB := connectLIMS()
	checkWorkflows(limsDB, mDB)
	checkWells(limsDB, mDB)
	checkPlates(limsDB, mDB)
	checkImages(limsDB, mDB)
}
