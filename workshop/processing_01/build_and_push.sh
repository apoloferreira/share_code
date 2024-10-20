%%sh

# Specify an algorithm name
image_name=custom-image-teste1

account=$(aws sts get-caller-identity --query Account --output text)

# Get the region defined in the current configuration (default to us-west-2 if none defined)
region=us-east-1

repository_base="${account}.dkr.ecr.${region}.amazonaws.com"
repository_uri="${repository_base}/${image_name}:latest"

# If the repository doesn't exist in ECR, create it.
aws ecr describe-repositories --repository-names "${algorithm_name}" > /dev/null 2>&1
if [ $? -ne 0 ]
then
aws ecr create-repository --repository-name "${algorithm_name}" > /dev/null
fi

# Get the login command from ECR and execute it directly
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${repository_base}

# Build the docker image locally with the image name and then push it to ECR with the full name.
docker build . -t ${image_name}:latest --file Dockerfile --repository ${image_name}:latest
docker tag ${image_name}:latest ${repository_uri}
docker push ${repository_uri}


# aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 891377318910.dkr.ecr.us-east-1.amazonaws.com/custom-image-teste1:latest
# docker build -t custom-image-teste1:latest .
# docker tag custom-image-teste1:latest 891377318910.dkr.ecr.us-east-1.amazonaws.com/custom-image-teste1:latest
# docker push 891377318910.dkr.ecr.us-east-1.amazonaws.com/custom-image-teste1:latest

# Error: saving credentials: error storing credentials - err: exit status 1, out: `error storing credentials - err: exit status 1, out: `pass not initialized: exit status 1: Error: password store is empty. Try "pass init".``
# Solution:
# gpg --generate-key
# pass init <generated gpg-id public key>
