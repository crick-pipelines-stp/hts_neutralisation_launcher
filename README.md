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
- miniconda3 (or other python installation with package manager)
- [plaque_assay](https://github.com/franciscrickinstitute/plaque_assay)


## To run:

To start all the celery workers:

```bash
# starts all processes in a tmux sesssion
./launch.sh
```

Then you'll need to create a cronjob for the snapshotter, which will detect new
exports and send them to the celery workers. An example of the cronjobs are
listed below:

```
# run neutralisation snapshot every 5 minutes to detect new files
*/5 * * * * source $HOME/.bashrc; $HOME/miniconda3/bin/python3.8 $HOME/launcher/launcher/run.py
*/5 * * * * source $HOME/.bashrc; $HOME/miniconda3/bin/python3.8 $HOME/launcher/launcher/run_titration.py
```

--------------


### Drag and drop portal
There is also a "drag-and-drop" version of the neutralisation analysis, which
enables running the analysis data exported to a specific directory, but saving
the output to files rather than uploading to the LIMS database. This is useful
for troubleshooting assays or other experiments which don't fit into the
typical neutralisation pipeline.

TODO: more info, where, how to set up.


### Dilution swapping
Sometimes during the assay, human error means dilutions are in the wrong
quadrants. There is a "dilution-swap-portal" which acts on the indexfiles and
can correct the positions of the dilutions to enable the indexfiles to then be
used in the normal plaque_assay analysis.

TODO: more info, where, how to set up.

