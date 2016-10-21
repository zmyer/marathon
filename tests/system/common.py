""" """
from shakedown import *
from utils import *

def app(id=1, instances=1):
    app_json = {
      "id": "",
      "instances":  1,
      "cmd": "sleep 100000000",
      "cpus": 0.01,
      "mem": 1,
      "disk": 0
    }
    if not str(id).startswith("/"):
        id = "/" + str(id)
    app_json['id'] = id
    app_json['instances'] = instances

    return app_json


def constraints(name, operator, value):
    constraints = [
        [
            name,
            operator,
            value
        ]
    ]
    return constraints


def pin_to_host(app_def, host):
    app_def['constraints'] = constraints('hostname','LIKE',host)


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


def delete_all_apps():
    client = marathon.create_client()
    apps = client.get_apps()
    for app in apps:
        if app['id'] == '/marathon-user':
            print('WARNING: marathon-user installed')
        else:
            client.remove_app(app['id'], True)


def delete_all_apps_wait():
    delete_all_apps()
    deployment_wait()


def deployment_wait():
    client = marathon.create_client()
    start = time.time()
    deployment_count = 1
    # TODO: time limit with fail
    while deployment_count > 0:
        time.sleep(1)
        deployments = client.get_deployments()
        deployment_count = len(deployments)

    end = time.time()
    elapse = round(end - start, 3)
    return elapse


def ip_other_than_mom():
    service_ips = get_service_ips('marathon', 'marathon-user')
    for mom_ip in service_ips:
        break

    agents = get_private_agents()
    for agent in agents:
        if agent != mom_ip:
            return agent

    return None


def ensure_mom():
    if not is_mom_installed():
        install_package_and_wait('marathon')
        deployment_wait()
        end_time = time.time() + 120
        while time.time() < end_time:
            if service_healthy('marathon-user'):
                break
            time.sleep(1)

        if not wait_for_service_url('marathon-user'):
            print('ERROR: Timeout waiting for endpoint')


def is_mom_installed():
    mom_ips = get_service_ips('marathon', "marathon-user")
    return len(mom_ips) != 0


def wait_for_service_url(service_name, timeout_sec=120):
    """Checks the service url if available it returns true, on expiration
    it returns false"""

    future = time.time() + timeout_sec
    url = dcos_service_url(service_name)
    while time.time() < future:
        response = None
        try:
            response = http.get(url)
        except Exception as e:
            pass

        if response == None:
            time.sleep(5)
        elif response.status_code == 200:
            return True
        else:
            time.sleep(5)

    return False

def wait_for_task(service, task, timeout_sec=120):
    """Waits for a task which was launched to be launched"""

    now = time.time()
    future = now + timeout_sec
    time.sleep(5)

    while now < future:
        response = None
        try:
            response = get_service_task(service, task)
        except Exception as e:
            pass

        if response is None:
            time.sleep(5)
            now = time.time()
        else:
            return response

    return None
