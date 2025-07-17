"""
Helper for kubernetes interactions (now using jobs instead of deployments).
"""

import re
import semver
import math
import uuid
import asyncio
import traceback
from datetime import datetime, timedelta
from loguru import logger
from typing import List, Dict, Any
from kubernetes import watch
from kubernetes.client import (
    V1Job,
    V1JobSpec,
    V1Service,
    V1ObjectMeta,
    V1PodTemplateSpec,
    V1PodSpec,
    V1Container,
    V1ResourceRequirements,
    V1ServiceSpec,
    V1ServicePort,
    V1Probe,
    V1EnvVar,
    V1Volume,
    V1VolumeMount,
    V1ConfigMapVolumeSource,
    V1ConfigMap,
    V1HostPathVolumeSource,
    V1SecurityContext,
    V1EmptyDirVolumeSource,
    V1ExecAction,
)
from kubernetes.client.rest import ApiException
from sqlalchemy import select
from api.exceptions import DeploymentFailure
from api.config import settings
from api.database import get_session
from api.server.schemas import Server
from api.chute.schemas import Chute
from api.deployment.schemas import Deployment
from api.config import k8s_core_client, k8s_batch_client, k8s_app_client


# Cache disk stats.
_disk_info_cache: dict[str, tuple[dict[str, float], datetime]] = {}
_disk_info_locks: dict[str, asyncio.Lock] = {}


def invalidate_node_disk_cache(node_name: str):
    """
    Invalidate the disk cache for a specific node.
    """
    if node_name in _disk_info_cache:
        logger.info(f"Invalidating cached disk size check for {node_name=}")
        del _disk_info_cache[node_name]


async def get_node_disk_info(node_name: str) -> Dict[str, float]:
    """
    Get disk space information for a specific node with caching.
    Returns dict with total_gb, available_gb, used_gb
    """
    # Check cache first
    if node_name in _disk_info_cache:
        disk_info, expiry_time = _disk_info_cache[node_name]
        if datetime.now() < expiry_time:
            return disk_info

    # Get or create a lock for this node
    if node_name not in _disk_info_locks:
        _disk_info_locks[node_name] = asyncio.Lock()

    async with _disk_info_locks[node_name]:
        if node_name in _disk_info_cache:
            disk_info, expiry_time = _disk_info_cache[node_name]
            if datetime.now() < expiry_time:
                return disk_info

        logger.info(f"Fetching fresh disk info for node {node_name}")
        try:
            pods = k8s_core_client().list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}"
            )
            used_disk_gb = 0
            for pod in pods.items:
                if pod.status.phase not in ["Running", "Pending"]:
                    continue

                # Check containers for ephemeral-storage requests
                if pod.spec.containers:
                    for container in pod.spec.containers:
                        if container.resources and container.resources.requests:
                            ephemeral_storage = container.resources.requests.get(
                                "ephemeral-storage", "0"
                            )
                            if isinstance(ephemeral_storage, str):
                                if ephemeral_storage.endswith("Gi"):
                                    used_disk_gb += float(ephemeral_storage.replace("Gi", ""))
                                elif ephemeral_storage.endswith("G"):
                                    used_disk_gb += float(ephemeral_storage.replace("G", ""))
                                elif ephemeral_storage.endswith("Mi"):
                                    used_disk_gb += (
                                        float(ephemeral_storage.replace("Mi", "")) / 1024
                                    )
                                elif ephemeral_storage.endswith("M"):
                                    used_disk_gb += float(ephemeral_storage.replace("M", "")) / 1024
                                elif ephemeral_storage.endswith("Ki"):
                                    used_disk_gb += (
                                        float(ephemeral_storage.replace("Ki", "")) / 1024 / 1024
                                    )

                # Also check init containers
                if pod.spec.init_containers:
                    for container in pod.spec.init_containers:
                        if container.resources and container.resources.requests:
                            ephemeral_storage = container.resources.requests.get(
                                "ephemeral-storage", "0"
                            )
                            if isinstance(ephemeral_storage, str):
                                if ephemeral_storage.endswith("Gi"):
                                    used_disk_gb += float(ephemeral_storage.replace("Gi", ""))
                                elif ephemeral_storage.endswith("G"):
                                    used_disk_gb += float(ephemeral_storage.replace("G", ""))
                                elif ephemeral_storage.endswith("Mi"):
                                    used_disk_gb += (
                                        float(ephemeral_storage.replace("Mi", "")) / 1024
                                    )
                                elif ephemeral_storage.endswith("M"):
                                    used_disk_gb += float(ephemeral_storage.replace("M", "")) / 1024
                                elif ephemeral_storage.endswith("Ki"):
                                    used_disk_gb += (
                                        float(ephemeral_storage.replace("Ki", "")) / 1024 / 1024
                                    )

            # Get node capacity
            node = k8s_core_client().read_node(name=node_name)

            # Try to get ephemeral storage capacity
            ephemeral_storage = node.status.capacity.get("ephemeral-storage", "0")
            if ephemeral_storage.endswith("Ki"):
                total_disk_gb = int(ephemeral_storage.replace("Ki", "")) / 1024 / 1024
            elif ephemeral_storage.endswith("Mi"):
                total_disk_gb = int(ephemeral_storage.replace("Mi", "")) / 1024
            elif ephemeral_storage.endswith("Gi"):
                total_disk_gb = int(ephemeral_storage.replace("Gi", ""))
            else:
                logger.warning(
                    "Could not determine node ephemeral storage capacity, using default=100"
                )
                total_disk_gb = 100

            # Reserve some disk space for system operations
            system_reserved_gb = 20
            available_disk_gb = total_disk_gb - used_disk_gb - system_reserved_gb
            disk_info = {
                "total_gb": total_disk_gb,
                "available_gb": max(0, available_disk_gb),
                "used_gb": used_disk_gb,
            }

            # Cache the result with 5 minute expiry
            expiry_time = datetime.now() + timedelta(minutes=5)
            _disk_info_cache[node_name] = (disk_info, expiry_time)
            logger.info(
                f"Node {node_name} disk info: total={total_disk_gb}GB, used={used_disk_gb}GB, available={available_disk_gb}GB"
            )
            return disk_info

        except Exception as e:
            logger.warning(f"Failed to get disk info for node {node_name}: {e}")
            error_result = {
                "total_gb": 0,
                "available_gb": 0,
                "used_gb": 0,
            }
            expiry_time = datetime.now() + timedelta(minutes=1)
            _disk_info_cache[node_name] = (error_result, expiry_time)

            return error_result


async def get_kubernetes_nodes() -> List[Dict]:
    """
    Get all Kubernetes nodes via k8s client, optionally filtering by GPU nodes.
    """
    nodes = []
    try:
        node_list = k8s_core_client().list_node(field_selector=None, label_selector="chutes/worker")
        for node in node_list.items:
            if not node.status.capacity or not node.status.capacity.get("nvidia.com/gpu"):
                logger.warning(f"Node has no GPU capacity: {node.metadata.name=}")
                continue
            gpu_count = int(node.status.capacity["nvidia.com/gpu"])
            gpu_mem_mb = int(node.metadata.labels.get("nvidia.com/gpu.memory", "32"))
            gpu_mem_gb = int(gpu_mem_mb / 1024)
            cpu_count = (
                int(node.status.capacity["cpu"]) - 2
            )  # leave 2 CPUs for incidentals, daemon sets, etc.
            cpus_per_gpu = (
                1 if cpu_count <= gpu_count else min(4, math.floor(cpu_count / gpu_count))
            )
            raw_mem = node.status.capacity["memory"]
            if raw_mem.endswith("Ki"):
                total_memory_gb = int(int(raw_mem.replace("Ki", "")) / 1024 / 1024) - 6
            elif raw_mem.endswith("Mi"):
                total_memory_gb = int(int(raw_mem.replace("Mi", "")) / 1024) - 6
            elif raw_mem.endswith("Gi"):
                total_memory_gb = int(raw_mem.replace("Gi", "")) - 6
            memory_gb_per_gpu = (
                1
                if total_memory_gb <= gpu_count
                else min(gpu_mem_gb, math.floor(total_memory_gb * 0.8 / gpu_count))
            )

            # Get disk space information
            disk_info = await get_node_disk_info(node.metadata.name)

            node_info = {
                "name": node.metadata.name,
                "validator": node.metadata.labels.get("chutes/validator"),
                "server_id": node.metadata.uid,
                "status": node.status.phase,
                "ip_address": node.metadata.labels.get("chutes/external-ip"),
                "cpu_per_gpu": cpus_per_gpu,
                "memory_gb_per_gpu": memory_gb_per_gpu,
                "disk_total_gb": disk_info.get("total_gb", 0),
                "disk_available_gb": disk_info.get("available_gb", 0),
                "disk_used_gb": disk_info.get("used_gb", 0),
            }
            nodes.append(node_info)
    except Exception as e:
        logger.error(f"Failed to get Kubernetes nodes: {e}")
        raise
    return nodes


async def check_node_has_disk_available(node_name: str, required_disk_gb: int) -> bool:
    """
    Check if a node has sufficient disk space available for a deployment.
    """
    disk_info = await get_node_disk_info(node_name)
    return disk_info.get("available_gb", 0) >= required_disk_gb


def is_job_ready(job):
    """
    Check if a job's pod is running and ready
    """
    # Get pods for this job
    pod_label_selector = f"chutes/deployment-id={job.metadata.labels.get('chutes/deployment-id')}"
    pods = k8s_core_client().list_namespaced_pod(
        namespace=job.metadata.namespace, label_selector=pod_label_selector
    )

    for pod in pods.items:
        if pod.status.phase == "Running":
            # Check if all containers are ready
            if pod.status.container_statuses:
                all_ready = all(cs.ready for cs in pod.status.container_statuses)
                if all_ready:
                    return True
    return False


def _extract_job_info(job: Any) -> Dict:
    """
    Extract job info from the job objects.
    """
    job_info = {
        "uuid": job.metadata.uid,
        "deployment_id": job.metadata.labels.get("chutes/deployment-id"),
        "name": job.metadata.name,
        "namespace": job.metadata.namespace,
        "labels": job.metadata.labels,
        "chute_id": job.metadata.labels.get("chutes/chute-id"),
        "version": job.metadata.labels.get("chutes/version"),
        "node_selector": job.spec.template.spec.node_selector,
    }

    # Job status information
    job_info["ready"] = is_job_ready(job)
    job_info["status"] = {
        "active": job.status.active or 0,
        "succeeded": job.status.succeeded or 0,
        "failed": job.status.failed or 0,
        "completion_time": job.status.completion_time,
        "start_time": job.status.start_time,
    }

    pod_label_selector = f"chutes/deployment-id={job.metadata.labels.get('chutes/deployment-id')}"
    pods = k8s_core_client().list_namespaced_pod(
        namespace=job.metadata.namespace, label_selector=pod_label_selector
    )
    job_info["pods"] = []
    for pod in pods.items:
        state = pod.status.container_statuses[0].state if pod.status.container_statuses else None
        last_state = (
            pod.status.container_statuses[0].last_state if pod.status.container_statuses else None
        )
        pod_info = {
            "name": pod.metadata.name,
            "phase": pod.status.phase,
            "restart_count": pod.status.container_statuses[0].restart_count
            if pod.status.container_statuses
            else 0,
            "state": {
                "running": state.running.to_dict() if state and state.running else None,
                "terminated": state.terminated.to_dict() if state and state.terminated else None,
                "waiting": state.waiting.to_dict() if state and state.waiting else None,
            }
            if state
            else None,
            "last_state": {
                "running": last_state.running.to_dict()
                if last_state and last_state.running
                else None,
                "terminated": last_state.terminated.to_dict()
                if last_state and last_state.terminated
                else None,
                "waiting": last_state.waiting.to_dict()
                if last_state and last_state.waiting
                else None,
            }
            if last_state
            else None,
        }
        job_info["pods"].append(pod_info)
        job_info["node"] = pod.spec.node_name
    return job_info


async def get_deployment(deployment_id: str):
    """
    Get a single job by deployment ID.
    """
    job = k8s_batch_client().read_namespaced_job(
        namespace=settings.namespace,
        name=f"chute-{deployment_id}",
    )
    return _extract_job_info(job)


get_job = get_deployment


async def get_deployed_chutes() -> List[Dict]:
    """
    Get all chutes jobs from kubernetes.
    """
    jobs = []
    label_selector = "chutes/chute=true"
    job_list = k8s_batch_client().list_namespaced_job(
        namespace=settings.namespace, label_selector=label_selector
    )
    for job in job_list.items:
        jobs.append(_extract_job_info(job))
        logger.info(f"Found chute job: {job.metadata.name} in namespace {job.metadata.namespace}")
    return jobs


async def get_deployed_chutes_legacy() -> List[Dict]:
    """
    Get all legacy chutes deployments (V1Deployment) from kubernetes.
    This is for backwards compatibility with the old deployment-based system.
    """
    deployments = []
    label_selector = "chutes/chute=true"
    try:
        deployment_list = k8s_app_client().list_namespaced_deployment(
            namespace=settings.namespace, label_selector=label_selector
        )
        for deployment in deployment_list.items:
            deploy_info = {
                "deployment_id": deployment.metadata.labels.get("chutes/deployment-id"),
                "name": deployment.metadata.name,
                "namespace": deployment.metadata.namespace,
                "labels": deployment.metadata.labels,
                "chute_id": deployment.metadata.labels.get("chutes/chute-id"),
                "version": deployment.metadata.labels.get("chutes/version"),
                "is_legacy": True,
            }
            deployments.append(deploy_info)
            logger.info(
                f"Found legacy chute deployment: {deployment.metadata.name} in namespace {deployment.metadata.namespace}"
            )
    except Exception as e:
        logger.error(f"Failed to get legacy deployments: {e}")
    return deployments


async def delete_code(chute_id: str, version: str):
    """
    Delete the code configmap associated with a chute & version.
    """
    try:
        code_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{chute_id}::{version}"))
        k8s_core_client().delete_namespaced_config_map(
            name=f"chute-code-{code_uuid}", namespace=settings.namespace
        )
    except ApiException as exc:
        if exc.status != 404:
            logger.error(f"Failed to delete code reference: {exc}")
            raise


async def wait_for_deletion(label_selector: str, timeout_seconds: int = 120):
    """
    Wait for a deleted pod to be fully removed.
    """
    if (
        not k8s_core_client()
        .list_namespaced_pod(
            namespace=settings.namespace,
            label_selector=label_selector,
        )
        .items
    ):
        logger.info(f"Nothing to wait for: {label_selector}")
        return

    w = watch.Watch()
    try:
        for event in w.stream(
            k8s_core_client().list_namespaced_pod,
            namespace=settings.namespace,
            label_selector=label_selector,
            timeout_seconds=timeout_seconds,
        ):
            if (
                not k8s_core_client()
                .list_namespaced_pod(
                    namespace=settings.namespace,
                    label_selector=label_selector,
                )
                .items
            ):
                logger.success(f"Deletion of {label_selector=} is complete")
                w.stop()
                break
    except Exception as exc:
        logger.warning(f"Error waiting for pods to be deleted: {exc}")


async def undeploy(deployment_id: str):
    """
    Delete a job, and associated service.
    """
    node_name = None
    try:
        job = k8s_batch_client().read_namespaced_job(
            name=f"chute-{deployment_id}",
            namespace=settings.namespace,
        )
        node_name = job.spec.template.spec.node_name
    except Exception:
        pass
    try:
        k8s_batch_client().delete_namespaced_job(
            name=f"chute-{deployment_id}",
            namespace=settings.namespace,
            propagation_policy="Foreground",
        )
    except Exception as exc:
        logger.warning(f"Error deleting job from k8s: {exc}")

    # Handle fallback to cleaning up old deployments, from instances
    # Created before the 2025-07-17 upgrade.
    if not node_name:
        try:
            k8s_app_client().delete_namespaced_deployment(
                name=f"chute-{deployment_id}",
                namespace=settings.namespace,
            )
        except Exception:
            ...

    await cleanup_service(deployment_id)
    await wait_for_deletion(f"chutes/deployment-id={deployment_id}", timeout_seconds=45)
    if node_name:
        invalidate_node_disk_cache(node_name)


async def get_pods_by_label(label_selector: str):
    """
    Get all pods matching a label selector.
    """
    try:
        pods = k8s_core_client().list_namespaced_pod(
            namespace=settings.namespace,
            label_selector=label_selector,
        )
        return [pod.to_dict() for pod in pods.items]
    except Exception as exc:
        logger.error(f"Error getting pods by label {label_selector}: {exc}")
        return []


async def create_code_config_map(chute: Chute):
    """
    Create a ConfigMap to store the chute code.
    """
    code_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{chute.chute_id}::{chute.version}"))
    config_map = V1ConfigMap(
        metadata=V1ObjectMeta(
            name=f"chute-code-{code_uuid}",
            labels={
                "chutes/chute-id": chute.chute_id,
                "chutes/version": chute.version,
            },
        ),
        data={chute.filename: chute.code},
    )
    try:
        k8s_core_client().create_namespaced_config_map(
            namespace=settings.namespace, body=config_map
        )
    except ApiException as e:
        if e.status != 409:
            raise


async def create_service_for_deployment(
    chute: Chute, deployment_id: str, extra_service_ports: list[dict[str, Any]] = []
):
    """
    Create a service for the specified deployment.
    """

    # And the primary chutes service.
    service = V1Service(
        metadata=V1ObjectMeta(
            name=f"chute-service-{deployment_id}",
            labels={
                "chutes/deployment-id": deployment_id,
                "chutes/chute": "true",
                "chutes/chute-id": chute.chute_id,
                "chutes/version": chute.version,
            },
        ),
        spec=V1ServiceSpec(
            type="NodePort",
            external_traffic_policy="Local",
            selector={
                "chutes/deployment-id": deployment_id,
            },
            ports=[
                V1ServicePort(port=8000, target_port=8000, protocol="TCP", name="chute-8000"),
                V1ServicePort(port=8001, target_port=8001, protocol="TCP", name="chute-8001"),
            ]
            + [
                V1ServicePort(
                    port=svc["port"],
                    target_port=svc["port"],
                    protocol=svc["proto"],
                    name=f"chute-{svc['port']}",
                )
                for svc in extra_service_ports
            ],
        ),
    )

    # Create, delete any that may have been successful upon failure.
    try:
        return k8s_core_client().create_namespaced_service(
            namespace=settings.namespace, body=service
        )
    except Exception:
        raise DeploymentFailure(
            f"Failed to create service for {chute.chute_id=} and {deployment_id=}"
        )


async def _deploy_chute(
    chute_id: str,
    server_id: str,
    deployment_id: str,
    service: Any,
    token: str = None,
    job_id: str = None,
    config_id: str = None,
    disk_gb: int = 10,
    extra_labels: dict[str, str] = {},
):
    """
    Deploy a chute as a Job!
    """
    # Backwards compatible types...
    if isinstance(chute_id, Chute):
        chute_id = chute_id.chute_id
    if isinstance(server_id, Server):
        server_id = server_id.server_id
    probe_port = 8000
    async with get_session() as session:
        chute = (
            (await session.execute(select(Chute).where(Chute.chute_id == chute_id)))
            .unique()
            .scalar_one_or_none()
        )
        server = (
            (await session.execute(select(Server).where(Server.server_id == server_id)))
            .unique()
            .scalar_one_or_none()
        )
        if not chute or not server:
            raise DeploymentFailure(f"Failed to find chute or server: {chute_id=} {server_id=}")

        # Determine the port to use for the liveness probe.
        core_version = re.match(
            r"^([0-9]+\.[0-9]+\.[0-9]+).*", (chute.chutes_version or "0.0.0")
        ).group(1)
        if semver.compare(core_version or "0.0.0", "0.3.3") >= 0:
            probe_port = 8001

        # Make sure the node has capacity.
        gpus_allocated = 0
        available_gpus = {gpu.gpu_id for gpu in server.gpus if gpu.verified}
        for deployment in server.deployments:
            gpus_allocated += len(deployment.gpus)
            available_gpus -= {gpu.gpu_id for gpu in deployment.gpus}
        if len(server.gpus) - gpus_allocated - chute.gpu_count < 0:
            raise DeploymentFailure(
                f"Server {server.server_id} name={server.name} cannot allocate {chute.gpu_count} GPUs, already using {gpus_allocated} of {len(server.gpus)}"
            )

        # Check disk space availability
        if not await check_node_has_disk_available(server.name, disk_gb):
            raise DeploymentFailure(
                f"Server {server.server_id} name={server.name} does not have {disk_gb}GB disk space available"
            )

        # Immediately track this deployment (before actually creating it) to avoid allocation contention.
        gpus = list([gpu for gpu in server.gpus if gpu.gpu_id in available_gpus])[: chute.gpu_count]
        gpu_uuids = [f"GPU-{str(uuid.UUID(gpu.gpu_id))}" for gpu in gpus]
        logger.info(
            f"Assigning {len(gpu_uuids)} GPUs [{gpu_uuids}] to {chute_id=} on {server.name=}"
        )
        deployment = Deployment(
            deployment_id=deployment_id,
            server_id=server.server_id,
            validator=server.validator,
            chute_id=chute.chute_id,
            version=chute.version,
            active=False,
            verified_at=None,
            stub=True,
            job_id=job_id,
            config_id=config_id,
        )
        session.add(deployment)
        deployment.gpus = gpus
        await session.commit()

    # Create the job.
    cpu = str(server.cpu_per_gpu * chute.gpu_count)
    ram = str(server.memory_per_gpu * chute.gpu_count) + "Gi"
    code_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{chute.chute_id}::{chute.version}"))
    deployment_labels = {
        "chutes/deployment-id": deployment_id,
        "chutes/chute": "true",
        "chutes/chute-id": chute.chute_id,
        "chutes/version": chute.version,
    }
    if config_id:
        deployment_labels["chutes/config-id"] = config_id

    # Command will vary depending on chutes version.
    extra_env = []
    command = [
        "chutes",
        "run",
        chute.ref_str,
        "--port",
        "8000",
    ]
    if not token:
        command += ["--graval-seed", str(server.seed)]
    else:
        extra_env += [
            V1EnvVar(
                name="CHUTES_LAUNCH_JWT",
                value=token,
            ),
            V1EnvVar(
                name="CHUTES_EXTERNAL_HOST",
                value=server.ip_address,
            ),
        ]

    # Port mappings must be in the environment variables.
    unique_ports = [8000, 8001]
    for port_object in service.spec.ports[2:]:
        proto = (port_object.protocol or "TCP").upper()
        extra_env.append(
            V1EnvVar(
                name=f"CHUTES_PORT_{proto}_{port_object.port}",
                value=str(port_object.node_port),
            )
        )
        if port_object.port not in unique_ports:
            unique_ports.append(port_object.port)

    # Tack on the miner/validator addresses.
    command += [
        "--miner-ss58",
        settings.miner_ss58,
        "--validator-ss58",
        server.validator,
    ]

    # Create the job object.
    job = V1Job(
        metadata=V1ObjectMeta(
            name=f"chute-{deployment_id}",
            labels=deployment_labels,
        ),
        spec=V1JobSpec(
            parallelism=1,
            completions=1,
            backoff_limit=0,
            ttl_seconds_after_finished=300,
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    labels=deployment_labels,
                    annotations={
                        "prometheus.io/scrape": "true",
                        "prometheus.io/path": "/_metrics",
                        "prometheus.io/port": "8000",
                    },
                ),
                spec=V1PodSpec(
                    restart_policy="Never",
                    node_name=server.name,
                    runtime_class_name="nvidia-container-runtime",
                    volumes=[
                        V1Volume(
                            name="code",
                            config_map=V1ConfigMapVolumeSource(
                                name=f"chute-code-{code_uuid}",
                            ),
                        ),
                        V1Volume(
                            name="cache",
                            host_path=V1HostPathVolumeSource(
                                path=f"/var/snap/cache/{chute.chute_id}",
                                type="DirectoryOrCreate",
                            ),
                        ),
                        V1Volume(
                            name="raw-cache",
                            host_path=V1HostPathVolumeSource(
                                path="/var/snap/cache",
                                type="DirectoryOrCreate",
                            ),
                        ),
                        V1Volume(
                            name="cache-cleanup",
                            config_map=V1ConfigMapVolumeSource(
                                name="chutes-cache-cleaner",
                            ),
                        ),
                        V1Volume(
                            name="tmp",
                            empty_dir=V1EmptyDirVolumeSource(size_limit=f"{disk_gb}Gi"),
                        ),
                        V1Volume(
                            name="shm",
                            empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit="16Gi"),
                        ),
                    ],
                    init_containers=[
                        V1Container(
                            name="cache-init",
                            image="parachutes/cache-cleaner:latest",
                            command=["/bin/bash", "-c"],
                            args=[
                                "mkdir -p /cache/hub /cache/civitai && chmod -R 777 /cache && python /scripts/cache_cleanup.py"
                            ],
                            env=[
                                V1EnvVar(
                                    name="CLEANUP_EXCLUDE",
                                    value=chute.chute_id,
                                ),
                                V1EnvVar(
                                    name="HF_HOME",
                                    value="/cache",
                                ),
                                V1EnvVar(
                                    name="CIVITAI_HOME",
                                    value="/cache/civitai",
                                ),
                                V1EnvVar(
                                    name="CACHE_MAX_AGE_DAYS",
                                    value=str(settings.cache_max_age_days),
                                ),
                                V1EnvVar(
                                    name="CACHE_MAX_SIZE_GB",
                                    value=str(
                                        settings.cache_overrides.get(
                                            server.name, settings.cache_max_size_gb
                                        )
                                    ),
                                ),
                            ],
                            volume_mounts=[
                                V1VolumeMount(name="raw-cache", mount_path="/cache"),
                                V1VolumeMount(
                                    name="cache-cleanup",
                                    mount_path="/scripts",
                                ),
                            ],
                            security_context=V1SecurityContext(
                                run_as_user=0,
                                run_as_group=0,
                            ),
                        ),
                    ],
                    containers=[
                        V1Container(
                            name="chute",
                            image=f"{server.validator.lower()}.localregistry.chutes.ai:{settings.registry_proxy_port}/{chute.image}",
                            image_pull_policy="Always",
                            env=[
                                V1EnvVar(
                                    name="NVIDIA_VISIBLE_DEVICES",
                                    value=",".join(gpu_uuids),
                                ),
                                V1EnvVar(
                                    name="CHUTES_PORT_PRIMARY",
                                    value=str(service.spec.ports[0].node_port),
                                ),
                                V1EnvVar(
                                    name="CHUTES_PORT_LOGGING",
                                    value=str(service.spec.ports[1].node_port),
                                ),
                                V1EnvVar(
                                    name="CHUTES_EXECUTION_CONTEXT",
                                    value="REMOTE",
                                ),
                                V1EnvVar(
                                    name="VLLM_DISABLE_TELEMETRY",
                                    value="1",
                                ),
                                V1EnvVar(
                                    name="NCCL_DEBUG",
                                    value="INFO",
                                ),
                                V1EnvVar(
                                    name="NCCL_SOCKET_IFNAME",
                                    value="^docker,lo",
                                ),
                                V1EnvVar(name="HF_HOME", value="/cache"),
                                V1EnvVar(name="CIVITAI_HOME", value="/cache/civitai"),
                            ]
                            + extra_env,
                            resources=V1ResourceRequirements(
                                requests={
                                    "cpu": cpu,
                                    "memory": ram,
                                    "ephemeral-storage": f"{disk_gb}Gi",
                                },
                                limits={
                                    "cpu": cpu,
                                    "memory": ram,
                                    "ephemeral-storage": f"{disk_gb}Gi",
                                },
                            ),
                            volume_mounts=[
                                V1VolumeMount(
                                    name="code",
                                    mount_path=f"/app/{chute.filename}",
                                    sub_path=chute.filename,
                                ),
                                V1VolumeMount(name="cache", mount_path="/cache"),
                                V1VolumeMount(name="tmp", mount_path="/tmp"),
                                V1VolumeMount(name="shm", mount_path="/dev/shm"),
                            ],
                            security_context=V1SecurityContext(
                                # XXX Would love to add this, but vllm (and likely other libraries) love writing files...
                                # read_only_root_filesystem=True,
                                capabilities={"add": ["IPC_LOCK"]},
                            ),
                            command=command,
                            ports=[{"containerPort": port} for port in unique_ports],
                            readiness_probe=V1Probe(
                                _exec=V1ExecAction(
                                    command=[
                                        "/bin/sh",
                                        "-c",
                                        f"curl -f http://127.0.0.1:{probe_port}/_alive || exit 1",
                                    ]
                                ),
                                initial_delay_seconds=15,
                                period_seconds=15,
                                timeout_seconds=1,
                                success_threshold=1,
                                failure_threshold=60,
                            ),
                        )
                    ],
                ),
            ),
        ),
    )

    # Create the job in k8s.
    try:
        created_job = k8s_batch_client().create_namespaced_job(
            namespace=settings.namespace, body=job
        )

        # Track the primary port in the database.
        deployment_port = service.spec.ports[0].node_port
        async with get_session() as session:
            deployment = (
                (
                    await session.execute(
                        select(Deployment).where(Deployment.deployment_id == deployment_id)
                    )
                )
                .unique()
                .scalar_one_or_none()
            )
            if not deployment:
                raise DeploymentFailure("Deployment disappeared mid-flight!")
            deployment.host = server.ip_address
            deployment.port = deployment_port
            deployment.stub = False
            await session.commit()
            await session.refresh(deployment)

        invalidate_node_disk_cache(server.name)
        return deployment, created_job
    except ApiException as exc:
        try:
            k8s_batch_client().delete_namespaced_job(
                name=f"chute-{deployment_id}",
                namespace=settings.namespace,
                propagation_policy="Foreground",
            )
        except Exception:
            ...
        raise DeploymentFailure(
            f"Failed to deploy chute {chute.chute_id} with version {chute.version}: {exc}\n{traceback.format_exc()}"
        )


async def cleanup_service(deployment_id: str):
    """
    Delete the k8s service associated with a deployment.
    """
    try:
        k8s_core_client().delete_namespaced_service(
            name=f"chute-service-{deployment_id}",
            namespace=settings.namespace,
        )
    except Exception as exc:
        logger.warning(f"Error removing primary service chute-service-{deployment_id}: {exc}")


async def deploy_chute(
    chute_id: str,
    server_id: str,
    deployment_id: str,
    service: Any,
    token: str = None,
    job_id: str = None,
    config_id: str = None,
    disk_gb: int = 10,
    extra_labels: dict[str, str] = {},
):
    """
    Exception handling deployment, such that if a deployment fails the service can be cleaned up.
    """
    try:
        return await _deploy_chute(
            chute_id,
            server_id,
            deployment_id,
            service,
            token=token,
            extra_labels=extra_labels,
            job_id=job_id,
            config_id=config_id,
            disk_gb=disk_gb,
        )
    except Exception as exc:
        logger.warning(
            f"Deployment of {chute_id=} on {server_id=} with {deployment_id=} {job_id=} failed, cleaning up service...: {exc=}"
        )
        await cleanup_service(deployment_id)
        raise
