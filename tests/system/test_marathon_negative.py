"""Marathon tests on DC/OS for negative conditions"""

import pytest
import time

from common import *
from shakedown import *
from utils import *


def test_pinned_task_scales_on_host_only():
    app_def = app('pinned')
    host = get_private_agents()[0]
    pin_to_host(app_def, host)

    with marathon_on_marathon():
        client = marathon.create_client()
        client.add_app(app_def)
        deployment_wait()

        tasks = client.get_tasks('/pinned')
        assert len(tasks) == 1
        assert tasks[0]['host'] == host

        client.scale_app('pinned', 10)
        deployment_wait()

        tasks = client.get_tasks('/pinned')
        assert len(tasks) == 10
        for task in tasks:
            assert task['host'] == host

def test_pinned_task_recovers_on_host():
    app_def = app('pinned')
    host = get_private_agents()[0]
    pin_to_host(app_def, host)

    with marathon_on_marathon():
        client = marathon.create_client()
        client.add_app(app_def)
        deployment_wait()
        tasks = client.get_tasks('/pinned')

        kill_process_on_host(host,'[s]leep')
        deployment_wait()
        new_tasks = client.get_tasks('/pinned')

        assert tasks[0]['id'] != new_tasks[0]['id']
        assert new_tasks[0]['host'] == host

def test_pinned_task_does_not_scale_to_unpinned_host():
    app_def = app('pinned')
    host = get_private_agents()[0]
    pin_to_host(app_def, host)
    # only 1 can fit on the node
    app_def['cpus'] = 3.5
    with marathon_on_marathon():
        client = marathon.create_client()
        client.add_app(app_def)
        deployment_wait()
        tasks = client.get_tasks('/pinned')
        client.scale_app('pinned', 2)
        # typical deployments are sub 3 secs
        time.sleep(5)
        deployments = client.get_deployments()
        tasks = client.get_tasks('/pinned')

        assert len(deployments) == 1
        assert len(tasks) == 1


# def setup_function(function):
#     with marathon_on_marathon():
#         delete_all_apps_wait()


def setup_module(module):
    cluster_info()

# def teardown_module(module):
#     with marathon_on_marathon():
#         delete_all_apps_wait()
