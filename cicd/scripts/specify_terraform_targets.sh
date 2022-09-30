#!/bin/bash 

set -e 

echo "the cntrs: $CONTAINERS"
TARGETS=""
#specify only the containers that changed
if [[ $CONTAINERS != "" ]]
then
  for c in $CONTAINERS
  do
    TARGETS="$TARGETS -target=azurerm_container_group.${c}_cg"
  done
fi

echo "the targs: $TARGETS"
echo "##vso[task.setvariable variable=TARGETS]$TARGETS"
