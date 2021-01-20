# Neutralisation analysis launcher

Launches plaque neutralisation analyses when new data is added to a directory.

Uses watchdog to monitor for filesystem changes, redis and celery to create an
analysis job-queue.


This requires an installation of redis-server, celery and a MySQL driver.


## Requirements
- Redis 6.0.10
- Celery 4.4.7
- Celery flower (optional)
- watchdog
- plaque_assay


## To run:
1. Start redis  
```
redis-server
```

2. Start celery in the working directory  
```
celery -A task worker -Q analysis --concurrency=1 --log-level=info -E
```

3. Start watchdog to monitor filesystem  
```
python main.py
```

(optional) flower to monitor celery jobs  
```
celery flower -A task --address=0.0.0.0 --port=5555
```


## To re-analyse previously run jobs:
Analysis jobs are stored in a local sqlite database to stop duplicate entries
into the LIMS. To re-analyse anything (in case of an error before LIMS upload) you have to
delete that entry from `processed_experiments.sqlite`

e.g if you want to delete the experiment "000EXAMPLE"
```
sqlite3 processed_experiments.sqlite

sqlite> DELETE FROM processed WHERE experiment="000EXAMPLE"
```
