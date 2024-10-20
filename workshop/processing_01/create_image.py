import sys
import argparse
import boto3
import subprocess
from botocore.client import BaseClient


def get_aws_account_number(client_sts: BaseClient) -> str:
    """
    Retrieves the AWS account number using STS client.
    """
    return client_sts.get_caller_identity()['Account']


def get_ecr_image_uri(account_id: str, region: str, algorithm_name: str, image_tag: str = 'latest'):
    """
    Constructs the full ECR image name.
    """
    fullname = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{algorithm_name}:{image_tag}"
    return fullname


def create_ecr_repository_if_not_exists(client_ecr: BaseClient, repository_name: str):
    """
    Checks if the ECR repository exists; if not, creates it.
    """
    try:
        client_ecr.describe_repositories(repositoryNames=[repository_name])
        print(f"ECR repository '{repository_name}' already exists.")
    except client_ecr.exceptions.RepositoryNotFoundException:
        client_ecr.create_repository(repositoryName=repository_name)
        print(f"Created ECR repository '{repository_name}'.")
    except Exception as e:
        print(f"Error checking or creating repository: {str(e)}")
        sys.exit(1)


def ecr_login(region: str, account_id: str):
    """
    Logs in to Amazon ECR Docker registry.
    """
    try:
        # Get ECR login password
        process = subprocess.run(
            ['aws', 'ecr', 'get-login-password', '--region', region],
            check=True, stdout=subprocess.PIPE, universal_newlines=True
        )
        password = process.stdout.strip()

        # Get the registry URI
        registry = f"{account_id}.dkr.ecr.{region}.amazonaws.com"

        # Login to Docker
        command = ['docker', 'login', '--username', 'AWS', '--password-stdin', registry]
        p = subprocess.Popen(command, stdin=subprocess.PIPE)
        p.communicate(input=password.encode())

        if p.returncode != 0:
            print("Docker login failed.")
            sys.exit(1)
        else:
            print("Docker login successful.")
    except Exception as e:
        print(f"Error logging into ECR: {str(e)}")
        sys.exit(1)


def build_docker_image(algorithm_name: str, dockerfile: str = 'Dockerfile', context: str = '.'):
    """
    Builds the Docker image locally.
    """
    command = [
        'docker', 'build', context, '-t', f"{algorithm_name}:latest",
        '--file', dockerfile
    ]
    result = subprocess.run(command)
    if result.returncode != 0:
        print("Docker build failed.")
        sys.exit(1)
    else:
        print(f"Docker image '{algorithm_name}:latest' built successfully.")


def tag_docker_image(algorithm_name: str, fullname: str):
    """
    Tags the Docker image with the ECR repository URI.
    """
    command = ['docker', 'tag', f"{algorithm_name}:latest", fullname]
    result = subprocess.run(command)
    if result.returncode != 0:
        print("Docker tag failed.")
        sys.exit(1)
    else:
        print(f"Docker image tagged as '{fullname}'.")


def push_docker_image(fullname: str):
    """
    Pushes the Docker image to Amazon ECR.
    """
    command = ['docker', 'push', fullname]
    result = subprocess.run(command)
    if result.returncode != 0:
        print("Docker push failed.")
        sys.exit(1)
    else:
        print(f"Docker image '{fullname}' pushed to ECR successfully.")


def main():
    parser = argparse.ArgumentParser(description='Build and push Docker image to AWS ECR')
    parser.add_argument('--algorithm-name',
        required=True,
        help='Name of the algorithm (ECR repository name)'
    )
    parser.add_argument('--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument('--image-tag',
        default='latest',
        help='Docker image tag (default: latest)'
    )
    parser.add_argument('--dockerfile',
        default='Dockerfile',
        help='Path to Dockerfile (default: Dockerfile)'
    )
    parser.add_argument('--context',
        default='.',
        help='Docker build context (default: current directory)'
    )

    args = parser.parse_args()
    algorithm_name = args.algorithm_name
    region = args.region
    image_tag = args.image_tag
    dockerfile = args.dockerfile
    context = args.context

    client_sts = boto3.client(service_name="sts", region_name=region)
    client_ecr = boto3.client(service_name="ecr", region_name=region)

    account_id = get_aws_account_number(client_sts)
    fullname = get_ecr_image_uri(account_id, region, algorithm_name, image_tag)

    create_ecr_repository_if_not_exists(client_ecr, algorithm_name)
    ecr_login(region, account_id)
    build_docker_image(algorithm_name, dockerfile, context)
    tag_docker_image(algorithm_name, fullname)
    push_docker_image(fullname)


if __name__ == "__main__":
    main()
