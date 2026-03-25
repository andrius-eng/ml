#!/usr/bin/env python3
"""
FlinkRunner Distributed Beam Job Test for Learning Distributed Computing

This script demonstrates how to use Apache Beam with Flink for distributed data processing.
It shows:
  - How to submit jobs to Flink
  - Monitoring job execution on the Flink jobmanager
  - Understanding parallelism and task distribution

Run with:
  python test_flink_runner.py --runner FlinkRunner --flink-master flink-jobmanager:8081
"""

import argparse
import sys
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run_word_count_pipeline(runner: str, flink_master: str, parallelism: int = 2):
    """
    Simple word count pipeline for learning distributed Beam + Flink.
    
    This pipeline:
    1. Creates sample data (wordswith their frequencies)
    2. Parallelizes processing across Flink task managers
    3. Aggregates results showing distributed execution
    
    Visit Flink UI while running: http://flink-jobmanager:8081
    """
    
    # Configure pipeline for Flink
    pipeline_args = [
        f"--runner={runner}",
        f"--flink_master={flink_master}",
        f"--parallelism={parallelism}",
        "--environment_type=LOOPBACK",  # Use LOOPBACK for reliable job submission
    ]
    
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    
    print(f"🚀 Starting Beam pipeline with {runner}")
    print(f"   Option: --runner={runner}")
    print(f"   Flink: {flink_master}")
    print(f"   Parallelism: {parallelism} workers")
    print(f"   Environment: LOOPBACK")
    print("\n📊 Watch the job execute at: http://flink-jobmanager:8081/jobs")
    print("=" * 60)
    
    with beam.Pipeline(options=options) as p:
        # 1. CREATE: Generate sample words with counts
        wordswith_count = (
            p
            | "CreateSampleData" >> beam.Create([
                ("distributed", 3),
                ("beam", 5),  
                ("flink", 2),
                ("computing", 4),
                ("distributed", 1),  # Duplicate to show aggregation
                ("flink", 3),
                ("learning", 2),
            ])
        )
        
        # 2. AGGREGATE: Group by word and sum counts (distributed across workers)
        aggregated = (
            wordswith_count
            | "GroupByWord" >> beam.CombinePerKey(sum)
        )
        
        # 3. FORMAT: Convert to readable output
        formatted = (
            aggregated
            | "FormatOutput" >> beam.Map(lambda item: f"{item[0]:15} : {item[1]:3} occurrences")
        )
        
        # 4. OUTPUT: Print results
        formatted | "PrintResults" >> beam.Map(print)
    
    print("\n" + "=" * 60)
    print("✅ Pipeline completed successfully!")
    print("\nThis demonstrates:")
    print("  • Job submission to Flink cluster")
    print("  • Parallel task execution across taskmanagers")
    print("  • Distributed data aggregation")
    print("  • Flink monitoring via REST API (port 8081)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test FlinkRunner with Apache Beam for distributed computing"
    )
    parser.add_argument(
        "--runner",
        default="DirectRunner",
        help="Beam runner: DirectRunner or FlinkRunner",
    )
    parser.add_argument(
        "--flink-master",
        default="localhost:8081",
        help="Flink jobmanager address (jobmanager-service:port)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=2,
        help="Parallelism level (number of parallel workers)",
    )
    
    args = parser.parse_args()
    
    run_word_count_pipeline(
        runner=args.runner,
        flink_master=args.flink_master,
        parallelism=args.parallelism
    )
