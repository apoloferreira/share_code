#!/bin/bash

# Specify an algorithm name
image_name=apolofs-geospatial-sm-distribution

# Set the region defined in the current configuration
region=us-east-1

account=$(aws sts get-caller-identity --query Account --output text)

repository_base="${account}.dkr.ecr.${region}.amazonaws.com"
repository_uri="${repository_base}/${image_name}:latest"

# If the repository doesn't exist in ECR, create it.
aws ecr describe-repositories --repository-names "${image_name}" > /dev/null 2>&1
if [ $? -ne 0 ]
then
aws ecr create-repository --repository-name "${image_name}" > /dev/null
fi

# Get the login command from ECR and execute it directly
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository_base}

# Build the docker image locally with the image name and then push it to ECR with the full name.
sm-docker build . --file Dockerfile \
    --repository ${image_name}:latest \
    --compute-type BUILD_GENERAL1_LARGE \
    --tag ${image_name}:latest

# sm-docker build . -t ${image_name}:latest --file Dockerfile --repository ${image_name}:latest
# sm-docker tag ${image_name}:latest ${repository_uri}
# sm-docker push ${repository_uri}
