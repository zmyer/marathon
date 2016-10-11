from shakedown import *


def test_overprovision():
    private_agents = get_private_agents()

    for agent in private_agents:
        provisioned = run_command(agent, "test -f over-provision.sh")
        if not provisioned:
            print("over-provisioning")
            stop_agent(agent)
            copy_file(agent, "./over-provision.sh")
            run_command(agent, "sh over-provision.sh")
            run_command(agent, "sudo rm -f /var/lib/mesos/slave/meta/slaves/latest")
            start_agent(agent)
