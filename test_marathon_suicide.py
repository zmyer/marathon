from shakedown import *

from dcos import config
from six.moves import urllib

toml_config_o = config.get_config()

def app( id = 1, instances = 1):
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

def deployment_wait():
    deployment_count = 1
    while deployment_count > 0:
        time.sleep(1)
        deployments =  client.get_deployments()
        deployment_count = len(deployments)

def test_scale_and_fail_marathon():
    app_json = app(1, 1000)
    client.add_app(app_json)
    print("1000 instances of 1 app deployed")
    deployment_wait()
    print("deployment complete")
    client.remove_app(app_json['id'], True)
    print("check logs marathon is down")
    reset_toml()

def update_marathon_client():
    global client
    global toml_config_o
    toml_config_o = config.get_config()
    dcos_url = config.get_config_val('core.dcos_url', toml_config_o)
    marathon_url = urllib.parse.urljoin(dcos_url, 'service/marathon-user/')
    config.set_val('marathon.url', marathon_url)
    toml_config_m = config.get_config()
    client = marathon.create_client(toml_config_m)

def reset_toml():
    global toml_config_o
    config.save(toml_config_o)

def setup_module(module):
    agents = get_private_agents()
    print("agents: {}".format(len(agents)))
    if len(agents)<9:
        assert False, "Incorrect Agent count: {}".format(len(agents))
    update_marathon_client()
