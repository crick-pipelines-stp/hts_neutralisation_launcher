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
	tasks := []AnalysisTask{}
	rows, err := db.con.Query("SELECT workflow_id, variant, created_at, finished_at FROM NE_task_tracking_analysis")
	if err != nil {
		log.Fatalln(err)
	}
	var task AnalysisTask
	var createdAt string
	var finishedAt string
	for rows.Next() {
		rows.Scan(&task.workflowID, &task.variant, &createdAt, &finishedAt)
		// go's stupid datetime parsing
		createdAtd, _ := time.Parse("2006-01-02 15:04:05", createdAt)
		finishedAtd, _ := time.Parse("2006-01-02 15:04:05", finishedAt)
		task.createdAt = createdAtd
		task.finishedAt = finishedAtd
		tasks = append(tasks, task)
	}
	return tasks
}

func (db Database) getStitchingTasks() []StitchingTask {
	tasks := []StitchingTask{}
	rows, err := db.con.Query("SELECT plate_name, created_at, finished_at FROM NE_task_tracking_stitching")
	if err != nil {
		log.Fatalln(err)
	}
	var task StitchingTask
	var createdAt string
	var finishedAt string
	for rows.Next() {
		rows.Scan(&task.plateName, &createdAt, &finishedAt)
		// go's stupid datetime parsing
		createdAtd, _ := time.Parse("2006-01-02 15:04:05", createdAt)
		finishedAtd, _ := time.Parse("2006-01-02 15:04:05", finishedAt)
		task.createdAt = createdAtd
		task.finishedAt = finishedAtd
		task.variant = db.getVariantName(task.plateName)
		task.workflowID = getWorkflowID(task.plateName)
		tasks = append(tasks, task)
	}
	return tasks
}

func (db Database) getStrains() {
	rows, err := db.con.Query("SELECT * FROM NE_available_strains")
	if err != nil {
		log.Fatalln(err)
	}

	var id int
	var mutant_strain string
	var plate_id_1 string
	var plate_id_2 string
	for rows.Next() {
		rows.Scan(&id, &mutant_strain, &plate_id_1, &plate_id_2)
		fmt.Println(id, mutant_strain, plate_id_1, plate_id_2)
	}
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
	// TODO: determine which column to check whether variant int is odd or even
	var variant string
	var varCode string
	varCode = platename[:3]
	if strings.HasPrefix(varCode, "T") {
		// convert titration plate to analysis plate just for looking up
		// variant
		varCode = strings.Replace(varCode, "T", "S", -1)
	}
	stmt, err := db.con.Prepare("SELECT mutant_strain FROM NE_available_strains WHERE plate_id_1 = ?")
	stmt.QueryRow(varCode).Scan(&variant)
	if err != nil {
		log.Fatalln(err)
	}
	if variant != "" {
		return variant
	}
	stmt2, err := db.con.Prepare("SELECT mutant_strain FROM NE_available_strains WHERE plate_id_2 = ?")
	if err != nil {
		log.Fatalln(err)
	}
	stmt2.QueryRow(varCode).Scan(&variant)
	return variant
}

func (analysis AnalysisTask) hasStitched(stitchings []StitchingTask) bool {
	matchCount := 0
	for _, stitch := range stitchings {
		if analysis.workflowID == stitch.workflowID && analysis.variant == stitch.variant {
			// TODO check actually has a finished at time
			matchCount++
		}
		if matchCount == 2 {
			return true
		}
	}
	return false
}

func findMissingStitching(analyses []AnalysisTask, stitchings []StitchingTask) (int, []AnalysisTask) {
	missing := []AnalysisTask{}
	for _, analysis := range analyses {
		if !analysis.hasStitched(stitchings) {
			missing = append(missing, analysis)
		}
	}
	return len(missing), missing
}

func sendSlackNotification(msg string) error {
	webhookURL := os.Getenv("SLACK_WEBHOOK_NEUTRALISATION")
	body := fmt.Sprintf("{'text': '%s'}", msg)
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBufferString(body))
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

func formatMsg(nFailed int, failures []AnalysisTask) string {
	msg := strings.Builder{}
	msg.WriteString(fmt.Sprintf(":warning: Found %d analyses with no stitched plates:\n", nFailed))
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
	nMissing, missing := findMissingStitching(analyses, stitchings)
	if nMissing > 0 {
		msg := formatMsg(nMissing, missing)
		err := sendSlackNotification(msg)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
