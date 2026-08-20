#!/usr/bin/env bash
# ==============================================================================
# JMeter Production Performance Runner
# ==============================================================================
set -euo pipefail

# --- Configuration & Paths ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT="$(pwd)"

CONTRACTS_VERSION="1.0.0-SNAPSHOT"

INPUT_JMX="${1:-eco_extreme_stress.jmx}"
if [[ "$INPUT_JMX" = /* ]]; then
    JMX_FILE="$INPUT_JMX"
else
    JMX_FILE="$SCRIPT_DIR/$INPUT_JMX"
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_JTL="$SCRIPT_DIR/reports/results_${TIMESTAMP}.jtl"
DASHBOARD_DIR="$SCRIPT_DIR/reports/dashboard_${TIMESTAMP}"

CONTRACTS_JAR="$PROJECT_ROOT/platform-common-contracts/target/platform-common-contracts-${CONTRACTS_VERSION}.jar"

echo "⚙️  [JMeter Boot] Packaging latest Protobuf contracts (Version: ${CONTRACTS_VERSION})...."
mvn clean install -pl platform-common-contracts -DskipTests

if [ ! -f "$CONTRACTS_JAR" ]; then
    echo "❌ FATAL: Compiled contracts JAR not found at $CONTRACTS_JAR"
    exit 1
fi

cd "$SCRIPT_DIR"

echo "🔧 [OS Tuning] Inspecting Linux system network stack limits..."
CURRENT_PORTS=$(sysctl -n net.ipv4.ip_local_port_range 2>/dev/null || echo "unknown")
CURRENT_REUSE=$(sysctl -n net.ipv4.tcp_tw_reuse 2>/dev/null || echo "unknown")

echo "   -> Current Port Range: $CURRENT_PORTS"
echo "   -> TCP Tw Reuse Value: $CURRENT_REUSE"

ulimit -n 65535 2>/dev/null || echo "⚠️  Could not raise ulimit file descriptors automatically."

echo "🚀 [JVM Tuning] Initializing performance heap structure..."
export HEAP="-Xms4g -Xmx4g -XX:MaxMetaspaceSize=512m"
export JVM_ARGS="-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Dlog4j2.formatMsgNoLookups=true -Dhttp.maxConnections=1000 -Dhttp.keepAlive=false"

echo "📦 [Classpath Discovery] Building full transitive runtime dependency list..."
DEPENDENCY_DIR="$PROJECT_ROOT/platform-common-contracts/target/jmeter-runtime-deps"
mkdir -p "$DEPENDENCY_DIR"

cd "$PROJECT_ROOT"
mvn dependency:copy-dependencies -pl platform-common-contracts -DoutputDirectory="$DEPENDENCY_DIR" -DincludeScope=runtime

if [ ! -f "$CONTRACTS_JAR" ]; then
    echo "❌ FATAL: Compiled contracts JAR not found at $CONTRACTS_JAR"
    exit 1
fi

COMBINED_CLASSPATH="${CONTRACTS_JAR}"
for jar in "$DEPENDENCY_DIR"/*.jar; do
    if [ -f "$jar" ]; then
        COMBINED_CLASSPATH="${COMBINED_CLASSPATH}:${jar}"
    fi
done

export JMETER_ARGS="-Jsearch_paths=${COMBINED_CLASSPATH} -Juser.classpath=${COMBINED_CLASSPATH}"

echo "📊 [Test Phase] Launching ${JMX_FILE} in Headless CLI Mode..."
mkdir -p "$SCRIPT_DIR/reports"

cd "$PROJECT_ROOT/bin/jmeter/groovy"

# shellcheck disable=SC2086
jmeter -n \
  -t "$JMX_FILE" \
  -l "$RESULTS_JTL" \
  -e -o "$DASHBOARD_DIR" \
  ${JMETER_ARGS}

echo "=============================================================================="
echo "✅ SUCCESS: Telemetry Stress Test Complete!"
echo "📈 Execution Logs (CSV Format): $RESULTS_JTL"
echo "🌐 HTML Production Dashboard File: $DASHBOARD_DIR/index.html"
echo "=============================================================================="