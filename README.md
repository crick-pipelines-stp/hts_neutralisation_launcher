# Neutralisation analysis launcher


Launches plaque neutralisation analyses when new data is exported.

- Monitors a given directory for new experiments in a cron job.
- When newly exported data is detected, submit analysis tasks to a job queue.


## File system monitoring
1. A cron job is used to list all the files matching a regex pattern in a given
CAMP directory.
2. Store a hash of the ordered filenames and the filenames themselves.
3. The next time the cron job is run, compare the hash to the previous value,
if it is different then identify new filenames.


## Job queue
The job queue is made with Celery and redis. There are separate queues for IC50
calculations and image stitching tasks. IC50 calculations are performed via the
`plaque_assay` library. Image stitching is done with a scikit-image scripts
and saved to a directory on CAMP.


## Requirements
This requires an installation of redis-server, celery and a MySQL driver.

- Redis 6.0.10
- Celery 4.4.7
- Celery flower (optional)
- [plaque_assay](https://github.com/franciscrickinstitute/plaque_assay)


## To run:

```bash
# starts all processes in a tmux sesssion
./launch.sh
```
