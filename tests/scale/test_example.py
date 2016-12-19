import time

from shakedown import *

from utils import file_dir


def test_agents():
    private_agents = sorted(get_private_agents())
    print('total agents: {}'.format(len(private_agents)))


def set_mom(name):
    try:
        marathons[name] = os.environ[name.upper()]
    except:
        marathons[name] = default_moms[name]
        pass


def setup_module(module):
    set_mom('mom1')
    set_mom('mom2')
    cluster_info()
    print('marathons in test: {}'.format(marathons))
