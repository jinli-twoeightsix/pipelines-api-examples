import argparse
import cromwell_driver
import logging
import os
import subprocess
import json
import sys

class Runner(object):
    def __init__(self, args, environ):
        self.args = args
        # Fetch all required environment variables, exiting if unset.
        self.environ = self.copy_from_env(
            ['CROMWELL',
             'CROMWELL_CONF'],
            environ)
        cromwell_conf = self.environ['CROMWELL_CONF']
        cromwell_jar = self.environ['CROMWELL']
        cromwell_driver.fill_cromwell_conf(cromwell_conf, self.args.gcs_working_dir)
        self.driver = cromwell_driver.CromwellDriver(cromwell_conf, cromwell_jar)

    def run(self):
        logging.info('starting')
        # Verify that the output directory is empty (or not there yet).
        err = self.output_dir_empty_or_missing(self.args.gcs_output_dir)
        if err:
            self.exit_with_error(err)

        (result_json, metadata_json) = self.driver.submit(self.args.wdl,
                                                          self.args.json)
        logging.info(result_json)
        metadata_filename = 'wdl_run_metadata.json'
        with open(metadata_filename, 'w') as f:
            json.dump(metadata_json, f)
        self.gsutil_cp([metadata_filename], self.args.gcs_output_dir)
        output_files = self.print_dict_values(result_json['outputs'], 'gs://')
        logging.info('copying if needed')
        if len(output_files) > 0:
            self.gsutil_cp(output_files, self.args.gcs_output_dir)

    def exit_with_error(self, err_string):
        sys.stderr.write(err_string + '\n')
        sys.exit(1)

    def gsutil_cp(self, source_files, dest_dir):
        cp_cmd = ['gsutil', 'cp']
        cp_cmd = cp_cmd + source_files
        cp_cmd.append(dest_dir)
        p = subprocess.Popen(cp_cmd, stderr=subprocess.PIPE)
        return_code = p.wait()
        if return_code:
            self.exit_with_error('copying files from {0} to {1} failed: {2}'.format(
                source_files, dest_dir, p.stderr.read()))

    def copy_from_env(self, env_vars, environ):
        result = {}
        for e in env_vars:
            val = environ.get(e, None)
            if val is None:
                exit_with_error('the ' + e + ' environment variable must be set')
            logging.info(e + '->' + os.environ[e])
            result[e] = val
        return result

    def output_dir_empty_or_missing(self, output_dir):
        p = subprocess.Popen(['gsutil', 'ls', '-l', output_dir], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        return_code = p.wait()
        # If the directory doesn't yet exist, create it.
        if return_code:
            p = subprocess.Popen(['gsutil', 'mkdir', output_dir], stderr=subprocess.PIPE)
            return_code = p.wait()
            if return_code:
                return None
            else:
                # We were unable to create the directory, return that error.
                return p.stderr.readlines()

        # The directory exists, verify that it's empty.
        result = p.stdout.readlines()
        error_str = 'output directory not empty: ' + output_dir
        if len(result) != 1:
            return error_str
        # There could be a single file, so we should make sure it's just the dir.
        # The easiest way to guarantee a match is to strip the trailing \n from the result
        # and normalize both paths.
        if os.path.normpath(output_dir) != os.path.normpath(result[0].strip()):
            return error_str
        return None

    def print_element(self, value, match_string):
        all_to_print = list()
        # The values can be singletons, lists, or dicts
        if isinstance(value, list):
            all_to_print += self.print_list_values(value, match_string)
        elif isinstance(value, unicode) or isinstance(value, str):
            if value.find(match_string) != -1:
                all_to_print.append(value)

        elif isinstance(value, dict):
            all_to_print += self.print_dict_values(value, match_string)
        else:
             # We don't search floats or bools.
             pass
        return all_to_print


    def print_dict_values(self, d, match_string):
        all_to_print = list()
        for key, value in d.viewitems():
            all_to_print += self.print_element(value, match_string)
        return all_to_print


    def print_list_values(self, l, match_string):
        all_to_print = list()
        for value in l:
            all_to_print += self.print_element(value, match_string)
        return all_to_print


def main():
    parser = argparse.ArgumentParser(description='Run WDLs')
    parser.add_argument('--wdl', required=True,
                        help='The WDL file to run')
    parser.add_argument('--json', required=True,
                        help='The JSON corresponding to the WDL file')
    parser.add_argument('--gcs_working_dir', required=True,
                        help='The location for Cromwell to put intermediate results.')
    parser.add_argument('--gcs_output_dir', required=True,
                        help='The location to put the final results.')

    args = parser.parse_args()

    # Write logs at info level
    logging.basicConfig(level=logging.INFO)
    # Don't info-log every new connection to localhost, to keep stderr small.
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
        logging.WARNING)

    runner = Runner(args, os.environ)
    runner.run()


if __name__ == '__main__':
    main()
