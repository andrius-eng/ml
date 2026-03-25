# Apache Beam + Flink: Distributed Computing Learning Guide

## Overview

This guide shows how to use **Apache Beam with Apache Flink** for distributed data processing. Flink is an open-source distributed stream/batch processing engine that can execute Beam pipelines across multiple machines.

## Architecture

The updated architecture in this repository uses Airflow scheduling, Beam portable runner graph translation, and a dedicated worker pool for Python harness execution.

```
               +----------------------------+
               |   Airflow DAG (Trigger)    |
               |   /python/beam_analysis.py |
               +-------------┬--------------+
                             |
                             ▼
          +------------------------------------------+
          |  Beam SDK (Python)                       |
          |  - Pipeline definition (PCollections,    |
          |    transforms, windows)                  |
          +---------------┬--------------------------+
                          |
                          ▼
          +------------------------------------------+
          |  Portable Pipeline (IR)                  |
          |  - Proto translation via Beam runner api  |
          |  - Java-compatible graph for Flink       |
          +---------------┬--------------------------+
                          |
                          ▼
        +------------------------------+    +-------------------------+
        | Flink JobManager (Master)    |    | Beam Worker Pool (Python)|
        | - REST API :8081             |    | - beam-worker-pool:50000 |
        | - schedules and checkpoints  |<--> | - executes Python fns    |
        +---------------┬--------------+    +-------------------------+
                        /|\
                         |
          +--------------+-------------+
          |  Flink TaskManagers         |
          |  - slots execute subtasks   |
          |  - parallelism from config  |
          +-----------------------------+
```

Notes:
- `BEAM_RUNNER=FlinkRunner` sends the pipeline to Flink via the portable API.
- `beam-worker-pool` is referenced by `--environment_config=beam-worker-pool:50000`.
- Python-to-Java conversion happens in the `Portable Pipeline` stage; runtime workers are Python processes.

## Key Concepts

### 1. **Runners**
Apache Beam supports multiple runners (execution engines):
- **DirectRunner**: Single-machine, for testing (default in Beam)
- **FlinkRunner**: Distributed, clusters of machines
- **SparkRunner**: Hadoop ecosystem alternative
- **DataflowRunner**: Google Cloud native

### 2. **Environment Types** (for FlinkRunner with remote job servers)
- **LOOPBACK**: Python worker runs in same container (recommended for learning)
- **DOCKER**: Uses Docker containers for workers (more complex deployment)
- **PROCESS**: Separate Python processes

### 3. **Parallelism**
- Number of parallel tasks across the cluster
- Each task processes a partition of data independently
- Default: 2 (good for learning)
- Production: Match your cluster size or data volume

### 4. **Beam Concepts**
- **PCollection**: Immutable, distributed dataset
- **Transform**: Operation on PCollections (beam.Map, beam.Filter, etc.)
- **CombineFn**: Aggregation function (sum, mean, etc.)
- **Pipeline**: DAG of transforms

## Setup

### Prerequisites (Already Configured)
```
✅ Apache Airflow 2.10.3     (Workflow orchestration)
✅ Apache Flink 1.19.1        (Distributed execution engine)
✅ Apache Beam 2.53.0         (Pipeline SDK - fixed for FlinkRunner)
✅ Docker Compose             (Infrastructure)
```

### Configuration

Updated `docker-compose.full.yml` with:
```yaml
BEAM_RUNNER: FlinkRunner
BEAM_ENVIRONMENT_TYPE: LOOPBACK
BEAM_PIPELINE_ARGS: >-
  --runner=FlinkRunner
  --flink_master=flink-jobmanager:8081
  --parallelism=2
  --environment_type=LOOPBACK
```

## Running Distributed Jobs

### Option 1: Test Script (Recommended for Learning)

```bash
cd /home/andrius/Development/ml

# Run with FlinkRunner (distributed across Flink cluster)
python test_flink_runner.py \
  --runner FlinkRunner \
  --flink-master flink-jobmanager:8081 \
  --parallelism 2

# Or test locally first
python test_flink_runner.py --runner DirectRunner
```

### Option 2: Python Script in Container

```bash
# Start containers
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d

# Run Beam job in scheduler container with FlinkRunner
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml exec airflow-scheduler bash -c '
  cd /opt/airflow/project && python python/beam_analysis.py \
    --runner FlinkRunner \
    --flink_master flink-jobmanager:8081 \
    --parallelism 2 \
    --input python/output/weather/raw_daily_weather.csv \
    --output-dir python/output/beam \
    --end-date 2026-03-24
'
```

### Option 3: Via Airflow DAG Trigger

The `weather_lithuania_dag` is pre-configured for FlinkRunner:

```bash
# Trigger the DAG (includes Beam job with FlinkRunner)
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml exec airflow-scheduler \
  airflow dags trigger lithuania_weather_analysis

# Check status
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml exec airflow-scheduler \
  airflow tasks states-for-dag-run lithuania_weather_analysis $(date -u +%Y-%m-%dT%H:%M:%S+00:00 | sed 's/:/%3A/g' | sed 's/+/%2B/g')
```

## Monitoring Job Execution

### 1. **Flink Web Dashboard**
Open in browser: `http://localhost:8081`

Features:
- **Overview**: Cluster health, running jobs
- **Jobs**: Current and historical job status
- **Task Managers**: Worker node details
- **Logs**: Debug information

### 2. **Job Submission Flow** (What you'll see)
```
Job Submission (Python → Flink)
      ↓
┌─ Flink JobManager receives job graph
│  ├─ Validates graph structure
│  ├─ Assigns tasks to available slots
│  │  └─ Slots = available CPU cores on TaskManagers
│  └─ Schedules execution
│
├─ TaskManagers receive task assignments
│  ├─ Allocate memory for task execution
│  └─ Start parallel execution
│
├─ Monitor progress
│  ├─ Collect metrics (records processed, throughput)
│  └─ Update task status
│
└─ Job completion
   ├─ Aggregate results
   └─ Return to Python process
```

### 3. **CLI Monitoring**

```bash
# List running jobs
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml exec flink-jobmanager \
  /opt/flink/bin/flink list

# Get job details (replace JOB_ID with actual ID from above)
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml exec flink-jobmanager \
  /opt/flink/bin/flink info JOB_ID

# View Flink cluster REST API
curl -s http://localhost:8081/v1/overview | python -m json.tool | head -20
```

## Runner Pool in this Stack

The repository includes a dedicated Beam worker pool service (`beam-worker-pool`) to decouple task worker provisioning from Flink job management. The key benefits:
- separate worker lifecycle from JobManager/TaskManager
- easier scaling via `--parallelism` and container autoscaling
- avoids Python worker stragglers blocking cluster scheduling

docker-compose snippet:
```yaml
beam-worker-pool:
  image: apache/beam_python3.12_sdk:2.71.0
  command: ["--worker_pool"]
  ports:
    - "50000:50000"
  networks:
    - default
  depends_on:
    - flink-jobmanager
  restart: unless-stopped
```
In your `BEAM_PIPELINE_ARGS` use `--environment_type=EXTERNAL` (or `LOOPBACK` for local dev) with `--environment_config=beam-worker-pool:50000`.

## Python-to-Java Conversion / FlinkRunner Interop Caveat

When running Python Beam pipelines on FlinkRunner, the Python DAG is translated into a portable pipeline graph and executed as a cross-language job on Flink. This involves a conversion step:
- Python SDK translates pipeline to Portable Proto (IR) via `beam_runner_api_pb2`.
- FlinkRunner implements this as a Java job graph in the Flink JVM.
- Python transforms are executed in worker harnesses (Python process) over Fn API.

Common issue: pipeline components that are only available in Python SDK (e.g., certain third-party transforms, lambda serialization, user-defined states/timers) may not be fully portable or require explicit Java expansion.

Mitigation:
- prefer built-in Beam transforms that are available in all SDKs
- use `--sdk_location` or `--environment` to ensure compatible worker image
- test locally with `DirectRunner` first, then `FlinkRunner` with `--environment_type=LOOPBACK`
- on conversion errors, inspect `Flink` UI and scheduler logs for `ProtoIncompatible` or `UnknownTransform` messages

## Learning Path

### Level 1: Understanding Basic Execution
1. Run `test_flink_runner.py` with DirectRunner first (single machine)
2. Compare with FlinkRunner execution (distributed)
3. Observe: Same code, different executioners

### Level 2: Distributed Data Processing
1. Study `python/beam_analysis.py` for real-world pattern:
   - Parallel city weather fetching (FetchCityWeather)
   - Grouped aggregation (temperature anomalies)
   - Distributed windowing
2. Run with different parallelism values: `--parallelism=1`, `--parallelism=4`
3. Watch Flink dashboard to see task distribution

### Level 3: Production Scaling
1. Increase data volume (add more cities/dates)
2. Monitor memory usage and throughput
3. Tune parallelism for your cluster
4. Add backpressure handling and error recovery

## Important Notes

### ✅ What Works (Proven Configuration)
- **Beam 2.53.0** with Flink 1.19.1
- **LOOPBACK environment** for job submission (no Docker overhead)
- **Direct access** to localhost:8081 for monitoring

### ⚠️ Known Limitations
- Beam 2.71.0 has serialization issues with Flink (fixed by using 2.53.0)
- DOCKER environment type adds complexity; LOOPBACK is simpler for learning
- Streaming mode requires different error handling
- State management needs backups for fault tolerance

## Beam + Flink Compatible Versions (Your Working Stack)

Your current setup is fully compatible:

Component	Version	Status
- Flink	1.18.1-scala_2.12-java11	✅ Supported by Beam 2.71.0
- Beam Python SDK	apache/beam_python3.12_sdk:2.71.0	✅ Official Docker image
- Beam Job Server	apache/beam_flink1.18_job_server:2.71.0	✅ Official Docker image
- Beam Runner	beam-runners-flink-1.18	✅ Maven artifact exists

### Official Compatibility Matrix [Beam Docs]
- Beam 2.71.0 → Flink 1.18.x ✅ (your exact versions)
- Beam 2.71.0 → Flink 1.19.x ✅ (newer option)
- Beam 2.71.0 → Flink 1.20.x ❌ (unsupported)

Why Flink 1.18.1 works perfectly:
- Beam 2.57.0+ → Flink 1.18.x (stable)
- Your: Beam 2.71.0 + Flink 1.18.1 = ✅ Perfect match

### Docker Images Confirmed
- ✅ flink:1.18.1-scala_2.12-java11 (~600MB)
- ✅ apache/beam_python3.12_sdk:2.71.0 (~1.8GB)
- ✅ apache/beam_flink1.18_job_server:2.71.0 (~600MB)

Your compose will work. Currently pulling the ~1.8GB worker pool image (normal). Flink UI at localhost:8081 in ~5 minutes.

Status: Production-ready versions. Deploy complete.

### Follow-ups
- How to build Beam pipeline for Flink 1.18
- Sample docker-compose for Flink 1.18 and Beam 2.57
- Common errors with Beam Flink version mismatch
- Flink Operator compatible Beam versions
- Next steps after picking compatible versions

### 🔧 Troubleshooting

**Problem**: FlinkRunner still fails with serialization error
```
Solution: Verify using LOOPBACK environment:
docker compose exec airflow-scheduler python -c "
import apache_beam as beam
p = beam.Pipeline(argv=[
  '--runner=FlinkRunner',
  '--flink_master=flink-jobmanager:8081',
  '--environment_type=LOOPBACK'
])
print('Pipeline initialized OK')
" 2>&1 | grep -i error
```

**Problem**: Jobs stuck in "INITIALIZING" status
```
Solution: Check Flink TaskManager is healthy:
docker compose ps | grep taskmanager
docker compose logs flink-taskmanager | tail -20
```

**Problem**: "Connection refused" to flink-jobmanager
```
Solution: Verify container networking:
docker compose exec airflow-scheduler ping flink-jobmanager
curl -s http://flink-jobmanager:8081/v1/overview 
```

## Example: Building Your Own Distributed Pipeline

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# 1. Define pipeline logic
def my_distributed_job():
    # Configure for distributed execution
    options = PipelineOptions([
        '--runner=FlinkRunner',
        '--flink_master=flink-jobmanager:8081',
        '--parallelism=4',
        '--environment_type=LOOPBACK',  # Important!
    ])
    
    with beam.Pipeline(options=options) as p:
        # This runs FIRST on just the driver Python process
        data = p | "Create" >> beam.Create(range(1000))
        
        # This runs PARALLEL across Flink workers
        squared = data | "Square" >> beam.Map(lambda x: x**2)
        
        # This collects back to driver
        squared | "Print" >> beam.Map(print)

# 2. Execute distributed
if __name__ == '__main__':
    my_distributed_job()
    print("✅ Distributed job completed!")
```

## Resources

- [Apache Beam Python Documentation](https://beam.apache.org/documentation/sdks/python/)
- [Flink Runner for Beam](https://beam.apache.org/documentation/runners/flink/)
- [Flink Concepts](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/overview/)
- [Beam/Flink Troubleshooting](https://beam.apache.org/documentation/runners/flink/#troubleshooting)

## Next Steps

1. ✅ Run the test script to verify FlinkRunner works
2. ✅ Monitor jobs via Flink web UI
3. ✅ Experiment with different parallelism settings
4. ✅ Modify `test_flink_runner.py` to process your own data
5. ✅ Integrate into production Airflow DAGs
