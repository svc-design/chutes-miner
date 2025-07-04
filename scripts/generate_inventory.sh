#!/usr/bin/env bash
set -euo pipefail

# This script generates ansible/inventory.yml based on environment variables.
# Required variables:
#   SSH_KEYS - comma separated list of SSH public keys
#   CPU_HOST, CPU_EXT_IP, CPU_WG_IP
#   GPU_HOST, GPU_EXT_IP, GPU_WG_IP

mkdir -p ansible

IFS=',' read -ra KEYS <<< "$SSH_KEYS"

cat > ansible/inventory.yml <<YAML
all:
  vars:
    ssh_keys:
YAML
for key in "${KEYS[@]}"; do
  echo "      - \"$key\"" >> ansible/inventory.yml
done
cat >> ansible/inventory.yml <<'YAML'
    user: root
    ansible_user: root
    validator: 5HEqivkDgz3hwoMGLVdkZ4PiMVgYtnn7bDPPH8TRaaDxoYpo
    is_primary: false
    gpu_enabled: true
    registry_port: 30500
    ansible_ssh_common_args: '-o ControlPath=none'
    ansible_ssh_retries: 3
    ubuntu_major: "22"
    ubuntu_minor: "04"
    cuda_version: "12-9"
    nvidia_version: "575"
    skip_cuda: false
    ipv6_enabled: true
    wireguard_mtu: 1380

  hosts:
    chutes-miner-cpu-0:
      ansible_host: ${CPU_HOST}
      external_ip: ${CPU_EXT_IP}
      wireguard_ip: ${CPU_WG_IP}
      gpu_enabled: false
      is_primary: true

    chutes-miner-gpu-0:
      ansible_host: ${GPU_HOST}
      external_ip: ${GPU_EXT_IP}
      wireguard_ip: ${GPU_WG_IP}
YAML
