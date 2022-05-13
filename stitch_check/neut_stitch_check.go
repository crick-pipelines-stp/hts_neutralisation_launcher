package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Database struct {
	con *sql.DB
}

type AnalysisTask struct {
	workflowID int
	variant    string
	createdAt  time.Time
	finishedAt time.Time
}

type StitchingTask struct {
	plateName  string
	createdAt  time.Time
	finishedAt time.Time
	workflowID int    // parsed form plateName
	variant    string // parsed from plateName + db query
}

const DATEFMT = "2006-01-02 15:04:05"

func connect(test bool) Database {
	// create connection to serology database
	user := os.Getenv("NE_USER")
	passwd := os.Getenv("NE_PASSWORD")
	var host string
	if test {
		host = os.Getenv("NE_HOST_TEST")
	} else {
		host = os.Getenv("NE_HOST_PROD")
	}
	database := "serology"
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, passwd, host, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalln(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}
	return Database{db}
}

func (db Database) getAnalysisTasks() []AnalysisTask {
	// get all analysis tasks as per NE_task_tracking_analysis table
	tasks := []AnalysisTask{}
	rows, err := db.con.Query(`
		SELECT
			workflow_id, variant, created_at, finished_at
		FROM
			NE_task_tracking_analysis
	`)
	if err != nil {
		log.Fatalln(err)
	}
	var task AnalysisTask
	var createdAt string
	var finishedAt string
	for rows.Next() {
		rows.Scan(&task.workflowID, &task.variant, &createdAt, &finishedAt)
		// go's stupid datetime parsing
		createdAtd, _ := time.Parse(DATEFMT, createdAt)
		finishedAtd, _ := time.Parse(DATEFMT, finishedAt)
		task.createdAt = createdAtd
		task.finishedAt = finishedAtd
		tasks = append(tasks, task)
	}
	return tasks
}

func (db Database) getStitchingTasks() []StitchingTask {
	// get all stitching tasks as per NE_task_tracking_stitching table
	taskCollection := []StitchingTask{}
	rows, err := db.con.Query(`
		SELECT
			plate_name, created_at, finished_at
		FROM
			NE_task_tracking_stitching
	`)
	if err != nil {
		log.Fatalln(err)
	}
	var task StitchingTask
	for rows.Next() {
		// variables declared each iteration otherwise null values in database
		// are incorrectly recorded as the last valid Time
		// So now null values are parsed as zero time, can be checked with
		// Time.IsZero()
		var createdAt string
		var finishedAt string
		rows.Scan(&task.plateName, &createdAt, &finishedAt)
		// go's stupid datetime parsing
		createdAtd, _ := time.Parse(DATEFMT, createdAt)
		finishedAtd, _ := time.Parse(DATEFMT, finishedAt)
		task.createdAt = createdAtd
		task.finishedAt = finishedAtd
		task.variant = db.getVariantName(task.plateName)
		task.workflowID = getWorkflowID(task.plateName)
		taskCollection = append(taskCollection, task)
	}
	return taskCollection
}

func getWorkflowID(platename string) int {
	// parse workflowID from platename
	workflowID, err := strconv.Atoi(platename[3:9])
	if err != nil {
		log.Fatalln(err)
	}
	return workflowID
}

func (db Database) getVariantName(platename string) string {
	// look up variant name from platename using variant integers
	var variant string
	var varCode string
	varCode = platename[:3]
	if strings.HasPrefix(varCode, "T") {
		// convert titration plate to analysis plate just for looking up
		// variant
		varCode = strings.Replace(varCode, "T", "S", -1)
	}
	stmt, err := db.con.Prepare(`
		SELECT
			mutant_strain
		FROM
			NE_available_strains
		WHERE
			plate_id_1 = ?  OR plate_id_2 = ?
	`)
	stmt.QueryRow(varCode, varCode).Scan(&variant)
	if err != nil {
		log.Fatalln(err)
	}
	return variant
}

func (s StitchingTask) isOld() bool {
	// check if a stitchingTask has been submitted more than 1 hour ago
	currTime := time.Now().UTC()
	diff := currTime.Sub(s.createdAt)
	return diff.Hours() > 1.0
}

func (s StitchingTask) isNew() bool {
	return !s.isOld()
}

func (s StitchingTask) failed() bool {
	// a stithing task has failed if it's old (createdAt > 1 hour ago) but has
	// not been marked as complete with a finishedAt time
	return s.isOld() && s.finishedAt.IsZero()
}

func (s StitchingTask) successful() bool {
	return !s.failed()
}

func (a AnalysisTask) sameWorkflowAs(s StitchingTask) bool {
	return a.workflowID == s.workflowID
}

func (a AnalysisTask) sameVariantAs(s StitchingTask) bool {
	return a.variant == s.variant
}

func (a AnalysisTask) sameAs(s StitchingTask) bool {
	return a.sameWorkflowAs(s) && a.sameVariantAs(s)
}

func (analysis AnalysisTask) hasStitched(stitchings []StitchingTask) bool {
	// determine if a single analysis has 2 corresponding stitched plates
	stitchPlateCount := 0
	for _, stitch := range stitchings {
		if analysis.sameAs(stitch) && (stitch.successful() || stitch.isNew()) {
			stitchPlateCount++
		}
		if stitchPlateCount == 2 {
			return true
		}
	}
	return false
}

func findMissingStitching(analyses []AnalysisTask, stitchings []StitchingTask) (bool, []AnalysisTask) {
	// return if there are analyses that don't have 2 stitched plates
	// and return list of those analyses
	missing := []AnalysisTask{}
	for _, analysis := range analyses {
		if !analysis.hasStitched(stitchings) {
			missing = append(missing, analysis)
		}
	}
	return len(missing) > 0, missing
}

func sendSlackNotification(msg string) error {
	// send slack notification with number of workflows + variants with missing
	// plates, and a list of them
	webhookURL := os.Getenv("SLACK_WEBHOOK_NEUTRALISATION")
	body := fmt.Sprintf("{'text': '%s'}", msg)
	resp, err := http.Post(
		webhookURL,
		"application/json",
		bytes.NewBufferString(body),
	)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		return errors.New(buf.String())
	}
	return nil
}

func formatMsg(failures []AnalysisTask) string {
	// create string suitable for slack message
	msg := strings.Builder{}
	nFailures := len(failures)
	header := fmt.Sprintf(":warning: Found %d analyses with <2 stitched plates:\n", nFailures)
	msg.WriteString(header)
	for _, f := range failures {
		msg.WriteString(fmt.Sprintf(" - %d  %s\n", f.workflowID, f.variant))
	}
	return msg.String()
}

func main() {
	db := connect(false)
	defer db.con.Close()
	analyses := db.getAnalysisTasks()
	stitchings := db.getStitchingTasks()
	hasMissing, missing := findMissingStitching(analyses, stitchings)
	if hasMissing {
		msg := formatMsg(missing)
		err := sendSlackNotification(msg)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
