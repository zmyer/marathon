import time

from shakedown import *
from utils import *


def app(id=1, instances=1):
    app_json = {
      "id": "",
      "instances":  1,
      "cmd": "for (( ; ; )); do sleep 100000000; done",
      "cpus": 0.01,
      "mem": 1,
      "disk": 0
    }
    if not str(id).startswith("/"):
        id = "/" + str(id)
    app_json['id'] = id
    app_json['instances'] = instances

    return app_json


def group(gcount=1, instances=1):
    id = "/test"
    group = {
        "id": id,
        "apps": []
    }

    for num in range(1, gcount + 1):
        app_json = app(id + "/" + str(num), instances)
        group['apps'].append(app_json)

    return group



def constraints(name, operator, value=None):
    constraints = [name, operator]
    if value is not None:
      constraints.append(value)
    return [constraints]


def unique_host_constraint():
    return constraints('hostname', 'UNIQUE')


def delete_all_apps():
    client = marathon.create_client()
    client.remove_group("/", True)
    time_deployment("undeploy")


def time_deployment(test=""):
    client = marathon.create_client()
    start = time.time()
    deployment_count = 1
    while deployment_count > 0:
        time.sleep(1)
        # troubling that we need this
        wait_for_service_endpoint('marathon-user')
        deployments = client.get_deployments()
        deployment_count = len(deployments)

    end = time.time()
    elapse = round(end - start, 3)
    return elapse


def delete_group(group="test"):
    client = marathon.create_client()
    client.remove_group(group, True)


def delete_group_and_wait(group="test"):
    delete_group(group)
    time_deployment("undeploy")

def deployment_less_than_predicate(count=10):
    client = marathon.create_client()
    return len(client.get_deployments()) < count

def launch_apps(count=1, instances=1):
    client = marathon.create_client()
    for num in range(1, count + 1):
        # after 400 and every 50 check to see if we need to wait
        if num > 400 and num % 50 == 0:
            client = marathon.create_client()
            count = len(client.get_deployments())
            if count > 30:
                # wait for deployment count to be less than 10
                wait_for(deployment_less_than_predicate)
                time.sleep(10)
        client.add_app(app(num, instances))


def launch_group(instances=1):
    client = marathon.create_client()
    client.create_group(group(instances))


def delete_all_apps_wait():
    delete_all_apps()
    time_deployment("undeploy")


def scale_apps(count=1, instances=1):
    test = "scaling apps: " + str(count) + " instances " + str(instances)

    start = time.time()
    launch_apps(count, instances)
    time_deployment(test)
    launch_time = elapse_time(start, time.time())
    delete_all_apps_wait()
    return launch_time


def scale_groups(instances=2):
    test = "group test count: " + str(instances)
    start = time.time()
    try:
        launch_group(instances)
    except:
        # at high scale this will timeout but we still
        # want the deployment time
        pass

    time_deployment(test)
    launch_time = elapse_time(start, time.time())
    delete_group_and_wait("test")
    return launch_time


def elapse_time(start, end=None):
    if end is None:
        end = time.time()
    return round(end-start, 3)


def cluster_info(mom_name='marathon-user'):
    agents = get_private_agents()
    print("agents: {}".format(len(agents)))
    client = marathon.create_client()
    about = client.get_about()
    print("marathon version: {}".format(about.get("version")))
    # see if there is a MoM
    with marathon_on_marathon(mom_name):
        try:
            client = marathon.create_client()
            about = client.get_about()
            print("marathon MoM version: {}".format(about.get("version")))

        except Exception as e:
            print("Marathon MoM not present")


def get_mom_json(version='v1.3.5'):
    mom_json = get_resource("mom.json")
    docker_image = "mesosphere/marathon:{}".format(version)
    mom_json['container']['docker']['image'] = docker_image
    mom_json['labels']['DCOS_PACKAGE_VERSION'] = version
    return mom_json


def install_mom(version='v1.3.5'):
    client = marathon.create_client()
    client.add_app(get_mom_json(version))
    deployment_wait()
