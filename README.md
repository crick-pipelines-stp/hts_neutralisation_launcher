# Neutralisation analysis launcher


Launches plaque neutralisation analyses when new data is added to a directory.

Uses watchdog to monitor for filesystem changes, redis and celery to create an
analysis job-queue.

![diagramme](diagramme.png)

## Requirements
This requires an installation of redis-server, celery and a MySQL driver.

- Redis 6.0.10
- Celery 4.4.7
- Celery flower (optional)
- watchdog
- plaque_assay


## To run:

```bash
# starts all processes in a tmux sesssion
./launch.sh
```

Or to run manually:

1. Start redis if not already running
```
redis-server
```

2. Start celery in the working directory  
```
celery -A task worker -Q analysis --concurrency=1 --loglevel=INFO -E -n analysis
celery -A task worker -Q image_stitch --concurrency=3 --loglevel=INFO -E -n image_stitcher
```

3. Start watchdog to monitor filesystem  
```
python main.py
```

(optional) flower to monitor celery jobs  
```
celery --broker=redis://localhost flower -A task --address=0.0.0.0 --port=5555 --basic_auth={username}:{password}
```


## To re-analyse previously run jobs:
A record of analyses and image stitching are stored in the serology LIMS
database in the `NE_task_tracking_analysis` and `NE_task_tracking_stitching`
tables respectively. If an entry here matches the workflow_id and variant then
that plate will not be re-analysed. Therefore to re-launch an analysis then
those entries will have to be removed from the tables.

N.B: there is also a check before uploading to the LIMS database that there is
not already an entry for a given workflow and variant. This will require
removing raw data from the database.
