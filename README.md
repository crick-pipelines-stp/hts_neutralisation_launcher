# Neutralisation analysis launcher

Launches plaque neutralisation analyses when new data is added to a directory.

Uses watchdog to monitor for filesystem changes, redis and celery to create a
job-queue.


Currently requires `plaque_assay` pacakge as a directory which is copied into the container
on build.


```bash
docker-compose build
docker-compose up
```
