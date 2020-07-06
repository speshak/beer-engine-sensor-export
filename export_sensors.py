#!/usr/bin/env python3
from stepfunctions_activity_worker import ActivityWorker
import logging
import requests
import tempfile
import os
import sys
import boto3
from datetime import datetime
from datetime import date

logger = logging.getLogger()
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
out_hdlr.setLevel(logging.INFO)
logger.addHandler(out_hdlr)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
GRAPH_HOST = os.environ['GRAPHITE_HOST']


def session_info(session_id):
    logger.info("Getting session metadata from info service")
    api = os.environ['INFO_API']
    res = requests.get(f"{api}sessions/{session_id}")
    res.raise_for_status()

    data = res.json()

    ret = {
            'ferm_id': data['fermenter'],
            'start': date.fromisoformat(data['brew_date']).strftime('%Y%m%d'),
        }

    ret['end'] = data.get('package_date', datetime.now().strftime('%Y%m%d'))

    return ret


def export_sensors(**task_input):
    info = session_info(task_input['session_id'])

    logger.info(f"Fetching sensor data for {info['ferm_id']} "
                f"({info['start']} - {info['end']})")
    url = (
            f"{GRAPH_HOST}/render/?target="
            f"stats.gauges.cbpi.fermenter.{info['ferm_id']}.*"
            f"&from={info['start']}&until={info['end']}"
            "&format=json"
        )

    res = requests.get(url, stream=True)
    res.raise_for_status()
    key = (f"{task_input['session_id']}/sensors-"
           f"{info['ferm_id']}-{info['start']}-{info['end']}.json")

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


def run_as_worker():
    activity_arn = os.environ['ACTIVITY_ARN']
    logger.info(f"Starting worker for {activity_arn}")
    worker = ActivityWorker(activity_arn, export_sensors)
    worker.listen()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        export_sensors(session_id=sys.argv[1])
    else:
        run_as_worker()
