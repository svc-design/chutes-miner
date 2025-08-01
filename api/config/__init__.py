"""
Application-wide settings.
"""

import os
import json
import redis.asyncio as redis
from functools import lru_cache
from typing import Any, List
from pydantic import BaseModel
from substrateinterface import Keypair
from pydantic_settings import BaseSettings
from kubernetes import client
from kubernetes.config import load_kube_config, load_incluster_config


class Validator(BaseModel):
    hotkey: str
    registry: str
    api: str
    socket: str


def create_kubernetes_client(cls: Any = client.CoreV1Api):
    """
    Create a k8s client.
    """
    try:
        if os.getenv("KUBERNETES_SERVICE_HOST") is not None:
            load_incluster_config()
        else:
            load_kube_config(config_file=os.getenv("KUBECONFIG"))
        return cls()
    except Exception as exc:
        raise Exception(f"Failed to create Kubernetes client: {str(exc)}")


@lru_cache(maxsize=1)
def k8s_core_client():
    return create_kubernetes_client()


@lru_cache(maxsize=1)
def k8s_app_client():
    return create_kubernetes_client(cls=client.AppsV1Api)


@lru_cache(maxsize=1)
def k8s_batch_client():
    return create_kubernetes_client(cls=client.BatchV1Api)


@lru_cache(maxsize=32)
def validator_by_hotkey(hotkey: str):
    valis = [validator for validator in settings.validators if validator.hotkey == hotkey]
    if valis:
        return valis[0]
    return None


class Settings(BaseSettings):
    _validators: List[Validator] = []
    sqlalchemy: str = os.getenv(
        "POSTGRESQL", "postgresql+asyncpg://user:password@127.0.0.1:5432/chutes"
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_client: redis.Redis = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    )
    netuid: int = int(os.getenv("NETUID", "64"))
    subtensor: str = os.getenv("SUBTENSOR_ADDRESS", "wss://entrypoint-finney.opentensor.ai:443")
    namespace: str = os.getenv("CHUTES_NAMESPACE", "chutes")
    graval_bootstrap_image: str = os.getenv(
        "GRAVAL_BOOTSTRAP_IMAGE",
        "parachutes/graval-bootstrap-opencl:0.2.6-cuda",
    )
    graval_bootstrap_image_rocm: str = os.getenv(
        "GRAVAL_BOOTSTRAP_IMAGE_ROCM",
        "parachutes/graval-bootstrap-opencl:0.2.6-rocm",
    )
    graval_bootstrap_timeout: int = int(os.getenv("GRAVAL_BOOTSTRAP_TIMEOUT", "900"))
    miner_ss58: str = os.environ["MINER_SS58"]
    miner_keypair: Keypair = Keypair.create_from_seed(os.environ["MINER_SEED"])
    validators_json: str = os.environ["VALIDATORS"]
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    registry_proxy_port: int = int(os.getenv("REGISTRY_PROXY_PORT", "30500"))
    prometheus_url: str = f"http://prometheus-server.{os.getenv('CHUTES_NAMESPACE', 'chutes')}.svc.cluster.local:{os.getenv('PROMETHEUS_PORT', '80')}"

    cache_max_age_days: int = int(os.getenv("CACHE_MAX_AGE_DAYS", "7"))
    cache_max_size_gb: int = int(os.getenv("CACHE_MAX_SIZE_GB", "500"))
    cache_overrides: dict = json.loads(os.getenv("CACHE_OVERRIDES", "{}")) or {}

    @property
    def validators(self) -> List[Validator]:
        if self._validators:
            return self._validators
        data = json.loads(self.validators_json)
        self._validators = [Validator(**item) for item in data["supported"]]
        return self._validators


settings = Settings()
