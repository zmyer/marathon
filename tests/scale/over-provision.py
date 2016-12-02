import time

from shakedown import *

from utils import file_dir


private_agents = sorted(get_private_agents())

for agent in private_agents:
    print(agent)
    stop_agent(agent)
    copy_file(agent, "{}/over-provision.sh".format(file_dir()))
    run_command(agent, "sh over-provision.sh")
    run_command(agent, "sudo rm -f /var/lib/mesos/slave/meta/slaves/latest")
    try:
        start_agent(agent)
    except:
        time.sleep(1)
        start_agent(agent)
