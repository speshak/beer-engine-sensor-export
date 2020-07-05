#!/usr/bin/env python3
from stepfunctions_activity_worker import ActivityWorker
import logging
import requests
import tempfile
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
GRAPH_HOST = os.environ['GRAPHITE_HOST']


def export_sensors(**task_input):
    ferm_id = task_input['fermenter_id']
    start = task_input['start']
    end = task_input['end']

    logger.info(f"Fetching sensor data for {ferm_id} ({start} - {end})")
    url = (
            f"{GRAPH_HOST}/render/?target="
            f"stats.gauges.cbpi.fermenter.{ferm_id}.*"
            f"&from={start}&until={end}"
            "&format=json"
        )

    res = requests.get(url, stream=True)
    res.raise_for_status()
    key = f"sensors/{ferm_id}-{start}-{end}.json"

    with tempfile.TemporaryFile() as f:
        for block in res.iter_content(1024):
            f.write(block)

        f.seek(0)
        s3_client.put_object(
                             Bucket=os.environ['BUCKET'],
                             Key=key,
                             ContentType="application/json",
                             Body=f
                            )
        logger.info(f"Wrote {key}")


if __name__ == '__main__':
    activity_arn = os.environ['ACTIVITY_ARN']
    logger.info(f"Starting worker for {activity_arn}")
    worker = ActivityWorker(activity_arn, export_sensors)
    worker.listen()
