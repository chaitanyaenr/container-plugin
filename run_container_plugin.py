#!/usr/bin/env python

import sys
import typing
from dataclasses import dataclass, field
from traceback import format_exc

from kubernetes import config, client
from kubernetes.client import V1PodList, V1Pod, ApiException, V1DeleteOptions
from arcaflow_plugin_sdk import validation, plugin, schema
from kubernetes.stream import stream

def setup_kubernetes(kubeconfig_path):
    if kubeconfig_path is None:
        kubeconfig_path = config.KUBE_CONFIG_DEFAULT_LOCATION
    kubeconfig = config.kube_config.KubeConfigMerger(kubeconfig_path)

    if kubeconfig.config is None:
        raise Exception(
            'Invalid kube-config file: %s. '
            'No configuration found.' % kubeconfig_path
        )
    loader = config.kube_config.KubeConfigLoader(
        config_dict=kubeconfig.config,
    )
    client_config = client.Configuration()
    loader.load_and_set(client_config)
    return client.ApiClient(configuration=client_config)

# list pods in a namespace matching label_selector
def list_pods(core_v1, namespace, label_selector=None):
    pods = []
    try:
        if label_selector:
            ret = core_v1.list_namespaced_pod(namespace, pretty=True, label_selector=label_selector)
        else:
            ret = core_v1.list_namespaced_pod(namespace, pretty=True)
    except ApiException as e:
        logging.error(
            "Exception when calling \
                       CoreV1Api->list_namespaced_pod: %s\n"
            % e
        )
        raise e
    for pod in ret.items:
        pods.append(pod.metadata.name)
    return pods

# Execute command in pod
def exec_cmd_in_pod(core_v1, command, pod_name, namespace, container=None, base_command="bash"):

    exec_command = [base_command, "-c", command]
    try:
        if container:
            ret = stream(
                core_v1.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                container=container,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        else:
            ret = stream(
                core_v1.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
    except Exception as e:
        raise e
    return ret


def get_containers_in_pod(core_v1, pod_name, namespace):
    pod_info = core_v1.read_namespaced_pod(pod_name, namespace)
    print(pod_info)
    container_names = []

    for cont in pod_info.spec.containers:
        container_names.append(cont.name)
    return container_names


@dataclass
class Container:
    namespace: str
    name: str
    container: str


@dataclass
class ContainerKillSuccessOutput:
    containers: typing.Dict[int, Container] = field(metadata={
        "name": "Containers removed",
        "description": ""
    })



@dataclass
class ContainerKillErrorOutput:
    error: str


@dataclass
class KillContainerConfig:
    """
    This is a configuration structure specific to container kill scenario. It describes which pod and container from which
    namespace to select for killing and how many to kill.
    """

    namespace: str = field(metadata={
        "name": "Namespace",
        "description": "Target pod namespace"
    })

    container_name: str = field(default=None, metadata={
        "name": "Container name",
        "description": "Target container in the pod."
    })

    command: str = field(default="kill 1", metadata={
        "name": "Command",
        "description": "Kill signal command to run to kill container, for example kill 1 or kill 9"
    })

    kill: typing.Annotated[int, validation.min(1)] = field(
        default=1,
        metadata={"name": "Number of pods to kill", "description": "How many pods should we attempt to kill?"}
    )

    label_selector: typing.Annotated[
        typing.Optional[str],
        validation.min(1),
        validation.required_if_not("name_pattern")
    ] = field(default=None, metadata={
        "name": "Label selector",
        "description": "Kubernetes label selector for the target pods. Required if name_pattern is not set.\n"
                       "See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ for details."
    })

    kubeconfig_path: typing.Optional[str] = field(default=None, metadata={
        "name": "Kubeconfig path",
        "description": "Path to your Kubeconfig file. Defaults to ~/.kube/config.\n"
                       "See https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/ for "
                       "details."
    })


@plugin.step(
    "kill-containers",
    "Kill containers",
    "Kill containers as specified by parameters",
    {"success": ContainerKillSuccessOutput, "error": ContainerKillErrorOutput}
)

def kill_containers(cfg: KillContainerConfig) -> typing.Tuple[str, typing.Union[ContainerKillSuccessOutput, ContainerKillErrorOutput]]:
    try:
        with setup_kubernetes(None) as cli:
            core_v1 = client.CoreV1Api(cli)

            pods = (core_v1, cfg.label_selector, cfg.namespace)
            if len(pods) < cfg.kill:
                return "error", ContainerKillErrorOutput(
                    "Not enough pods match the criteria, expected {} but found only {} pods".format(cfg.kill, len(pods))
                )
            
            killed_containers: typing.Dict[int, Container] = {}

            for i in range(cfg.kill):
                pod = pods[i]
                containers = get_containers_in_pod(core_v1, pod, cfg.namespace)
                if cfg.container_name in containers:
                    exec_cmd_in_pod(core_v1, cfg.command, pod, cfg.namespace, cfg.container_name, base_command="bash")
                else:
                    return "error", ContainerKillErrorOutput("Cannot find the specified container in the pods matching the label, please check")
                c = Container(
                    pod.metadata.namespace,
                    pod.metadata.name,
                    container_name
                )
                killed_containers[int(time.time_ns())] = c
              
        return "success", ContainerKillSuccessOutput(killed_containers)

    except Exception:
        return "error", ContainerKillErrorOutput(
            format_exc()
        )

if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        kill_containers,
    )))
