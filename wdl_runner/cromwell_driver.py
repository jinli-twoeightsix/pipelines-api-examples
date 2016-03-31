import json
import logging
import os
import requests
import select
import string
import subprocess
import sys
import time
import urllib2


class CromwellDriver:
    def __init__(self, cromwell_conf, cromwell_jar):
        self.p = subprocess.Popen(['java', '-Dconfig.file=' + cromwell_conf, '-jar', cromwell_jar, 'server'])
        # Wait for Cromwell to start.
        # TODO: Verify that Cromwell has started. We don't want to check Cromwell's logs because we want
        #  those to 'pass through' so the user can see them easily. Perhaps a better way would be to do a get
        #  request after some time to verily Cromwell is up.
        time.sleep(10)
        logging.info('Started Cromwell')

    def exit_with_error(self, err_string):
        sys.stderr.write(err_string + '\n')
        sys.exit(1)

    def fetch(self, wf_id=None, post=False, files=None, method=None):
        url = 'http://localhost:8000/api/workflows/v1'
        if wf_id is not None:
            url = os.path.join(url, wf_id)
        if method is not None:
            url = os.path.join(url, method)
        if post:
            r = requests.post(url, files=files)
        else:
            r = requests.get(url)
        return r.json()

    def submit(self, wdl, json_file, sleep_time=15):
        # Post to the server with the job to run.
        files = {'wdlSource': open(wdl, 'rb'), 'workflowInputs': open(json_file, 'rb')}
        j = self.fetch(post=True, files=files)
        cromwell_id = j['id']
        if j['status'] != 'Submitted':
            self.exit_with_error('Status from Cromwell for job submission was not Submitted, instead ' + j['status'])
        logging.info('id is ' + cromwell_id)
        loop = 0
        while True:
            time.sleep(sleep_time)
            status_json = self.fetch(wf_id=cromwell_id, method='status')
            status = status_json['status']
            if status == 'Succeeded':
                break
            elif status == 'Running':
                pass
            else:
                self.exit_with_error('Status of job is not Running or Succeeded: ' + status)
            loop += 1
        logging.info('Succeeded')
        metadata = self.fetch(wf_id=cromwell_id, method='outputs')
        outputs = self.fetch(wf_id=cromwell_id, method='outputs')
        return outputs, metadata


def fill_cromwell_conf(conf_file, gcs_working_dir):
    req = urllib2.Request('http://metadata.google.internal/computeMetadata/v1/project/project-id', None,
                          {'Metadata-Flavor': 'Google'})
    project_id = urllib2.urlopen(req).read()
    # Fill the cromwell_conf file
    with open(conf_file, 'rb') as f:
        conf_data = f.read()
        template_to_fill = string.Template(conf_data)
        new_conf_data = template_to_fill.safe_substitute(project_id=project_id, gcs_working_dir=gcs_working_dir)
    with open(conf_file, 'wb') as f:
        f.write(new_conf_data)


if __name__ == '__main__':
    pass

