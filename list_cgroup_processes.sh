#!/bin/bash

CGROUP_DIR="/sys/fs/cgroup/cpu"

for CGROUP in $(find $CGROUP_DIR -mindepth 1 -maxdepth 1 -type d); do
  CGROUP_NAME=$(basename $CGROUP)
  echo "CGroup: $CGROUP_NAME"
  while read PID; do
    if [ -n "$PID" ]; then
      ps -fp $PID
    fi
  done < $CGROUP/cgroup.procs
  echo ""
done
