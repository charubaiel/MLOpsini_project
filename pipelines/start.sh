#!/bin/bash

if [ ! -d "$PWD/.DH" ] 
then

mkdir .DH &&
cat <<EOF > $PWD'/.DH/dagster.yaml'
telemetry:
    enabled: false
run_coordinator:
    module: dagster.core.run_coordinator
    class: QueuedRunCoordinator
EOF

fi
DAGSTER_HOME=$PWD'/.DH' dagster dev



