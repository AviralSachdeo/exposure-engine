#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

echo "═══════════════════════════════════════════════════════════"
echo " Compiling project..."
echo "═══════════════════════════════════════════════════════════"
./mvnw -q compile -DskipTests

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Starting Exposure Engine Latency Benchmark"
echo "═══════════════════════════════════════════════════════════"

MAVEN_OPTS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED" \
  ./mvnw -q exec:java \
  -Dexec.mainClass="com.github.programmingwithmati.exposureEngine.loadTestExposureEngine.ExposureEngineLatencyBenchmark" \
  -Dexec.classpathScope=compile
