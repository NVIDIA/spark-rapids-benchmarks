#!/usr/bin/env bash
set -euo pipefail

# One-command smoke test for nds_gen_data_spark.py on Minikube.
# It verifies Spark-on-K8s submission and checks generated table outputs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NDS_DIR="${REPO_ROOT}/nds"

SPARK_HOME="${SPARK_HOME:-$HOME/local/spark}"
SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
K8S_IMAGE="${K8S_IMAGE:-spark-py:v3.5.4-py312}"
ARCHIVE_PATH="${NDS_DIR}/tpcds-gen/target/lib/dsdgen.tar.gz"
OUTPUT_ROOT="${OUTPUT_ROOT:-/tmp/nds_shared}"
OUTPUT_DIR="${OUTPUT_ROOT}/output"
MOUNT_PID_FILE="${OUTPUT_ROOT}/.minikube_mount.pid"
MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY_MB="${MINIKUBE_MEMORY_MB:-8192}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "ERROR: file not found: $1" >&2
    exit 1
  fi
}

echo "[1/7] Checking prerequisites..."
require_cmd minikube
require_cmd docker
require_cmd "${SPARK_SUBMIT}"
require_file "${NDS_DIR}/nds_gen_data_spark.py"
require_file "${NDS_DIR}/Dockerfile.spark-k8s"

if [[ ! -f "${ARCHIVE_PATH}" ]]; then
  echo "ERROR: ${ARCHIVE_PATH} not found." >&2
  echo "Build and package dsdgen first:" >&2
  echo "  cd nds/tpcds-gen" >&2
  echo "  make clean all LINUX_CC='gcc -fcommon'" >&2
  echo "  cd target && tar czf lib/dsdgen.tar.gz tools/" >&2
  exit 1
fi

if ! docker ps >/dev/null 2>&1; then
  echo "ERROR: docker is not accessible for current user." >&2
  echo "Run: sudo usermod -aG docker \$USER && newgrp docker" >&2
  exit 1
fi

export PATH="${SPARK_HOME}/bin:${PATH}"

echo "[2/7] Starting minikube (if needed)..."
if ! minikube status >/dev/null 2>&1; then
  minikube start --cpus="${MINIKUBE_CPUS}" --memory="${MINIKUBE_MEMORY_MB}" --driver=docker
fi

echo "[3/7] Ensuring Spark service account/role..."
minikube kubectl -- create serviceaccount spark >/dev/null 2>&1 || true
minikube kubectl -- create clusterrolebinding spark-role \
  --clusterrole=edit --serviceaccount=default:spark >/dev/null 2>&1 || true

echo "[4/7] Starting shared mount for local output..."
mkdir -p "${OUTPUT_ROOT}"
chmod 777 "${OUTPUT_ROOT}"
if [[ -f "${MOUNT_PID_FILE}" ]]; then
  old_pid="$(cat "${MOUNT_PID_FILE}" || true)"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" >/dev/null 2>&1; then
    kill "${old_pid}" >/dev/null 2>&1 || true
  fi
fi
nohup minikube mount "${OUTPUT_ROOT}:${OUTPUT_ROOT}" --uid 185 --gid 0 \
  >"${OUTPUT_ROOT}/minikube_mount.log" 2>&1 &
echo $! > "${MOUNT_PID_FILE}"
sleep 3

echo "[5/7] Building Spark image inside minikube docker daemon..."
eval "$(minikube docker-env)"
docker build -t "${K8S_IMAGE}" -f "${NDS_DIR}/Dockerfile.spark-k8s" "${SPARK_HOME}"

echo "[6/7] Running Spark data generation smoke test..."
rm -rf "${OUTPUT_DIR}"
"${SPARK_SUBMIT}" \
  --master "k8s://$(minikube kubectl -- config view --minify -o jsonpath='{.clusters[0].cluster.server}')" \
  --deploy-mode client \
  --conf "spark.kubernetes.container.image=${K8S_IMAGE}" \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=spark" \
  --conf "spark.executor.instances=2" \
  --conf "spark.kubernetes.container.image.pullPolicy=Never" \
  --conf "spark.pyspark.python=python3" \
  --conf "spark.pyspark.driver.python=python3" \
  --conf "spark.kubernetes.executor.volumes.hostPath.nds-data.mount.path=${OUTPUT_ROOT}" \
  --conf "spark.kubernetes.executor.volumes.hostPath.nds-data.options.path=${OUTPUT_ROOT}" \
  --archives "${ARCHIVE_PATH}#dsdgen" \
  "${NDS_DIR}/nds_gen_data_spark.py" 1 2 "${OUTPUT_DIR}" --overwrite

echo "[7/7] Verifying generated output..."
table_count="$(ls -d "${OUTPUT_DIR}"/*/ 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${table_count}" != "25" ]]; then
  echo "ERROR: expected 25 source table directories, got ${table_count}" >&2
  exit 1
fi
if ! head -1 "${OUTPUT_DIR}/store_sales/"*.txt >/dev/null 2>&1; then
  echo "ERROR: store_sales data not found or empty." >&2
  exit 1
fi

echo "Smoke test passed."
echo "Output dir: ${OUTPUT_DIR}"
echo "Table count: ${table_count}"
