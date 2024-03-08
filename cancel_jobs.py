 # Copyright (C) 2023 Google Inc.
 #
 # Licensed under the Apache License, Version 2.0 (the "License"); you may not
 # use this file except in compliance with the License. You may obtain a copy of
 # the License at
 #
 # http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 # WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 # License for the specific language governing permissions and limitations under
 # the License.

import asyncio
import argparse

from google.cloud import bigquery


client = bigquery.Client()


async def cancel_job(
    job_id,
    location: str = "us"):
    job = client.cancel_job(job_id, location=location)
    print(f"requested cancellation for job {job.location}:{job.job_id}.")


def returnState(job):
  if job.state == "DONE":
    if job.error_result is None:
      return "SUCCESSFUL"
    elif job.error_result['reason'] == u'stopped':
      return "CANCELLED"
    else:
      return "FAILED"
  else:
    return job.state


async def list_jobs_to_cancel(project, states = ["RUNNING", "PENDING"]):
    jobs = [job for job in client.list_jobs(project=project, max_results=1000, all_users=True)]
    for state in states:
        return [job.job_id for job in jobs if returnState(job) in state]


async def main(project, provided_jobs=[]):
    jobs_to_cancel = provided_jobs if provided_jobs else await list_jobs_to_cancel(project)
    if not jobs_to_cancel:
        print("no bq jobs to cancel.")
        return
    print(f"will request cancellation for jobs: {jobs_to_cancel}")
    await asyncio.gather(
        *[cancel_job(id) for id in jobs_to_cancel])


if __name__ ==  '__main__':
    parser = argparse.ArgumentParser(description='Cancel BigQuery Jobs')
    parser.add_argument('--project', required=True,
                    help='gcp project to cancel bq jobs.')
    parser.add_argument('jobs', metavar='JOBS', type=str, nargs='*',
                    help='bq jobs ids to cancel.')
    parser.add_argument('--pending',
                    action='store_true',
                    help='cancel only pending jobs.')

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.project, args.jobs))
    loop.close()