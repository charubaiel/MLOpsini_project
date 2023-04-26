#!/bin/bash

mkdir .DH &&
cat <<EOF > $PWD'/.DH/dagster.yaml'
telemetry:
  enabled: false
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
EOF