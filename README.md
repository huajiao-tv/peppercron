# PepperCron - Distributed job scheduling system

PepperCron is a distributed cron job management system

## Documentation

### Add Job

- PATH: `/v1/jobs`
- METHOD: `POST`
- Content-Type: `application/json`
- Request:

```json
{
  "name": "echo_job",
  "type": 0,
  "times": 0,
  "schedule": "@every 1m",
  "executor_type": 0,
  "executor_params": ["sleep", "2"],
  "executor_timeout": 1000000000,
  "environment_variables": [],
  "tag": "test",
  "dependent_jobs": [],
  "concurrency": 2
}
```

### Delete Job

- PATH: `/v1/jobs/<job_name>`
- METHOD: `DELETE`

## Development Quick start

### docker

Run the Docker Compose config:

```/bin/bash
cd deploy
docker-compose up -d --build --scale peppercron=2
```

The above commands will create one etcd and 2 peppercron instances.

Check peppercron port mapping using `docker-compose ps`
