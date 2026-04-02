#!/usr/bin/env python3
"""
PortableRunner + Flink: distributed Beam word-count demo.

Submits a simple word-count pipeline to Flink via beam-job-server (PortableRunner).

Prerequisites — stack must already be running:
  docker compose --project-directory . \
    -f airflow/docker-compose.yml -f docker-compose.full.yml up -d

Run from inside the scheduler container:
  docker exec airflow-airflow-scheduler-1 \
    python3 /opt/airflow/project/test_flink_runner.py

Or override endpoints for local testing:
  python test_flink_runner.py \
    --job-endpoint beam-job-server:8099 \
    --artifact-endpoint beam-job-server:8098 \
    --environment-config localhost:50000
"""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run_word_count_pipeline(
    runner: str,
    job_endpoint: str,
    artifact_endpoint: str,
    environment_config: str,
    parallelism: int = 1,
):
    """
    Simple word-count pipeline that runs distributed on Flink via PortableRunner.

    Pipeline flow:
      1. Create sample word data
      2. Combine counts per word (distributed across Flink task slots)
      3. Print results

    Monitor at: http://localhost:8082  (Flink UI, host port)
    """
    pipeline_args = [
        f"--runner={runner}",
    ]

    if runner == "PortableRunner":
        pipeline_args += [
            f"--job_endpoint={job_endpoint}",
            f"--artifact_endpoint={artifact_endpoint}",
            "--environment_type=EXTERNAL",
            f"--environment_config={environment_config}",
            f"--parallelism={parallelism}",
        ]

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    print(f"Runner         : {runner}")
    if runner == "PortableRunner":
        print(f"Job endpoint   : {job_endpoint}")
        print(f"Artifact ep    : {artifact_endpoint}")
        print(f"Env config     : {environment_config}")
        print(f"Parallelism    : {parallelism}")
        print("Monitor jobs   : http://localhost:8082")
    print("-" * 50)

    with beam.Pipeline(options=options) as p:
        words = (
            p
            | "CreateSampleData" >> beam.Create([
                ("distributed", 3),
                ("beam", 5),
                ("flink", 2),
                ("computing", 4),
                ("distributed", 1),
                ("flink", 3),
                ("learning", 2),
            ])
        )

        aggregated = words | "SumPerWord" >> beam.CombinePerKey(sum)

        aggregated | "Print" >> beam.Map(
            lambda item: print(f"{item[0]:15} : {item[1]:3} occurrences")
        )

    print("-" * 50)
    print("Pipeline completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Word-count demo via PortableRunner → beam-job-server → Flink"
    )
    parser.add_argument(
        "--runner",
        default="PortableRunner",
        choices=["PortableRunner", "DirectRunner"],
        help="Beam runner (default: PortableRunner)",
    )
    parser.add_argument(
        "--job-endpoint",
        default="beam-job-server:8099",
        help="beam-job-server gRPC endpoint",
    )
    parser.add_argument(
        "--artifact-endpoint",
        default="beam-job-server:8098",
        help="beam-job-server artifact endpoint",
    )
    parser.add_argument(
        "--environment-config",
        default="localhost:50000",
        help="beam-worker-pool address (localhost:50000 inside shared netns)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Flink parallelism — keep at 1 with a single TaskManager",
    )

    args = parser.parse_args()
    run_word_count_pipeline(
        runner=args.runner,
        job_endpoint=args.job_endpoint,
        artifact_endpoint=args.artifact_endpoint,
        environment_config=args.environment_config,
        parallelism=args.parallelism,
    )
