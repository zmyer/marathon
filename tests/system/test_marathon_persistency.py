"""Marathon tests on DC/OS for negative conditions"""

import pytest
import time
import uuid

from common import *
from shakedown import *
from utils import *
from dcos import *


def test_launch_container_with_presistent_volume():

    with marathon_on_marathon():
        app_def = peristent_volume_app()
        client = marathon.create_client()
        client.add_app(app_def)
        deployment_wait()

        tasks = client.get_tasks(app_id)
        assert len(tasks) == 1

        port = tasks[0]['ports'][0]
        host = tasks[0]['host']
        cmd = "curl {}:{}/data/foo".format(host, port)
        run, data = run_command_on_master(cmd)

        assert run, "{} did not succeed".format(cmd)
        assert data == 'hello\n', "'{}' was not equal to hello\\n".format(data)

        client.restart_app(app_id)
        deployment_wait()

        tasks = client.get_tasks(app_id)
        assert len(tasks) == 1

        port = tasks[0]['ports'][0]
        host = tasks[0]['host']
        cmd = "curl {}:{}/data/foo".format(host, port)
        run, data = run_command_on_master(cmd)

        assert run, "{} did not succeed".format(cmd)
        assert data == 'hello\nhello\n', "'{}' was not equal to hello\\nhello\\n".format(data)

def teardown_module(module):

    with marathon_on_marathon():
        client = marathon.create_client()
        client.remove_group("/", True)
        deployment_wait()
