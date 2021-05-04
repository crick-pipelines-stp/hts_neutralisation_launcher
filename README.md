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
1. Start redis  
```
redis-server --port 7777
```

2. Start celery in the working directory  
```
celery -A task worker -Q analysis --concurrency=1 --loglevel=INFO -E -n analysis
celery -A task worker -Q image_stitch --concurrency=8 --loglevel=INFO -E -n image_stitcher
```

3. Start watchdog to monitor filesystem  
```
python main.py
```

(optional) flower to monitor celery jobs  
```
celery flower -A task --address=0.0.0.0 --port=5555 --basic_auth={username}:{password}
```


## To re-analyse previously run jobs:
Analysis jobs are stored in a local sqlite database to stop duplicate entries
into the LIMS. To re-analyse anything (in case of an error before LIMS upload) you have to delete that entry from `processed_experiments.sqlite`

### CLI tool
This will remove workflow ID 101 from the `processed` table (all variants).
```bash
./remove_processed 101
```

To specify the variant:
```bash
./remove_processed 101 --variant a
```
```bash
./remove_processed 101 -v a b
```

To remove stitched entries (all variants):
```bash
./remove_stitched 101
```


### Manually

e.g if you want to delete the workflow_id "000101"
```
sqlite3 processed_experiments.sqlite

sqlite> DELETE FROM processed WHERE experiment="000101";
sqlite> DELETE FROM stitched WHERE plate_name LIKE "%000101";
```
