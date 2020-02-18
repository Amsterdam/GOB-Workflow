"""Calling this module will run the end-to-end tests.

The end-to-end test tests importing, comparing, storing and relating of entities.

Program exit code is the number of errors that occurred, or -1 in case a system error occurred.

"""

import requests
import os

from time import sleep

from gobcore.status.heartbeat import STATUS_END, STATUS_REJECTED
from gobworkflow.config import API_HOST
from gobworkflow.workflow.config import IMPORT, RELATE
from gobworkflow.storage.storage import job_get, connect
from gobworkflow.workflow.workflow import Workflow


class E2ETest:
    """Class E2ETest

    Tests importing, comparing, updating and relating of entities. Uses GOB-API to verify the correctness of the
    result.

    """
    MAX_WAIT_JOB_FINISHED = 3600  # Seconds, roughly

    test_catalog = "test_catalogue"

    test_import_entity = "test_entity"
    test_import_sources = [
        "DELETE_ALL",
        "ADD",
        "MODIFY1",
        "DELETE_ALL",
        "ADD",
        "MODIFY1",
    ]

    test_relation_entities = [
        "rel_test_entity_a",
        "rel_test_entity_b",
        "rel_test_entity_c",
        "rel_test_entity_d",
    ]

    test_relation_src_entities = [
        "rel_test_entity_a",
        "rel_test_entity_b",
    ]

    entities_abbreviations = {
        "rel_test_entity_a": "rta",
        "rel_test_entity_b": "rtb",
    }

    test_relation_dst_relations = [
        'rtc_ref_to_c',
        'rtc_manyref_to_c',
        'rtd_ref_to_d',
        'rtd_manyref_to_d'
    ]

    api_base = f"{API_HOST}/gob"

    check_import_endpoint = "/test_catalogue/test_entity/?ndjson=true"

    def __init__(self):
        self.failure_cnt = 0

    def _start_import(self, catalog: str, collection: str, application: str):
        job = Workflow(IMPORT).start_new({
            'catalogue': catalog,
            'collection': collection,
            'application': application,
        })
        self._wait_job_finished(job)

    def _start_relate(self, catalog: str, collection: str, attribute: str):
        job = Workflow(RELATE).start_new({'catalogue': catalog, 'collection': collection, 'attribute': attribute})
        self._wait_job_finished(job)

    def _wait_job_finished(self, job):
        job = job_get(job['id'])
        if job.status == STATUS_REJECTED:
            return self._exit_error(f"Job {job.id} rejected")

        cnt = 0
        while cnt < self.MAX_WAIT_JOB_FINISHED:
            job = job_get(job.id)

            if job.status == STATUS_END:
                return

            sleep(1)
            cnt += 1

        self._exit_error(f"Job {job.id} took too long to finish")

    def _check_api_output(self, endpoint: str, testfile: str, step_name: str):
        def sort_lines(data: str):
            return "\n".join(sorted(data.split("\n")))

        expected_data = self._load_testfile(testfile)
        r = requests.get(f"{self.api_base}{endpoint}")

        if r.status_code != 200:
            return self._exit_error(f"Error requesting {endpoint}")

        success = sort_lines(r.text) == sort_lines(expected_data)

        self._log_result(step_name, success)

    def _log(self, msg: str):
        print(msg)

    def _exit_error(self, msg: str):
        self._log(msg)
        exit(-1)

    def _log_result(self, step: str, success: bool):
        def red(text: str):
            return f'\033[31m{text}\033[0m'

        def green(text: str):
            return f'\033[32m{text}\033[0m'

        if success:
            self._log(f"{step:<40} {green('OK')}")
        else:
            self._log(f"{step:<40} {red('FAIL')}")
            self.failure_cnt += 1

    def _load_testfile(self, filename: str):
        """Returns content of test file in expect directory

        """
        with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'expect', filename)) as f:
            return f.read()

    def _test_imports(self):
        self._log("Test imports")

        for source in self.test_import_sources:
            self._start_import(self.test_catalog, self.test_import_entity, source)
            self._check_api_output(self.check_import_endpoint, f"expect.{source}.ndjson", f"Import {source}")

    def _test_relations(self):
        self._log("Test relations")

        # First import entities to relate
        for entity in self.test_relation_entities:
            self._start_import(self.test_catalog, entity, 'REL')

        for src_entity in self.test_relation_src_entities:
            for dst_rel in self.test_relation_dst_relations:
                self._start_relate(self.test_catalog, src_entity, '_'.join(dst_rel.split('_')[1:]))

                rel_entity = f"tst_{self.entities_abbreviations[src_entity]}_tst_{dst_rel}"
                self._check_api_output(
                    f"/dump/rel/{rel_entity}/?format=csv",
                    f"expect.{rel_entity}.ndjson",
                    f"Relation {rel_entity}"
                )

    def run(self):
        self._test_imports()
        self._test_relations()

        return self.failure_cnt


def run_e2e_tests():
    if __name__ == "__main__":
        connect()
        e2e = E2ETest()
        error_cnt = e2e.run()
        exit(error_cnt)


run_e2e_tests()
