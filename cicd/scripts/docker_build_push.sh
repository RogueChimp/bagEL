#!/bin/bash 

#####################################################################
###############  build and push updated images  #####################
#####################################################################

DOCKERFILE_DIR=$1
TAG=$2

#check for updated sources
if [[ $IS_PR != "" ]]
then
  SRCS=$(git diff $TARGET_BRANCH...$SOURCE_BRANCH --name-only | grep -i sources/ | sed -r 's/sources\/|\/.+//g' | sort | uniq)
else
  SRCS=$(git log -n 1 --name-only | grep -i sources/ | sed -r 's/sources\/|\/.+//g' | sort | uniq)
fi

printf "=======\nupdated sources:$(wc -l <<<$SRCS)\n$SRCS\n=======\n"

#remove newlines
SRCS=$(echo $SRCS | tr '\n' ' ')
echo "##vso[task.setvariable variable=CONTAINERS]$SRCS"

#build and push updated source images
for src in $SRCS
do
  ACR_TAG=${CONTAINERREGISTRY}.azurecr.io/$src:$TAG

  printf "building $ACR_TAG ...\n"

  docker build -f $DOCKERFILE_DIR/Dockerfile --label com.azure.dev.image.system.teamfoundationcollectionuri=$SYSTEM_TEAMFOUNDATIONCOLLECTIONURI --label com.azure.dev.image.system.teamproject=$SYSTEM_TEAMPROJECT  --label com.azure.dev.image.build.repository.name=$BUILD_REPOSITORY_NAME  --label com.azure.dev.image.build.sourceversion=$BUILD_SOURCEVERSION  --label com.azure.dev.image.build.repository.uri=$BUILD_REPOSITORY_URI  --label com.azure.dev.image.build.sourcebranchname=$BUILD_SOURCEBRANCHNAME  --label com.azure.dev.image.build.definitionname=$BUILD_DEFINITIONNAME  --label com.azure.dev.image.build.buildnumber=$BUILD_BUILDNUMBER  --label com.azure.dev.image.build.builduri=$BUILD_BUILDURI --build-arg SRC=$src -t $ACR_TAG $BUILD_SOURCESDIRECTORY

  docker push $ACR_TAG
done
