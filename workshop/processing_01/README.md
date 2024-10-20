

## Container Build and push
In this example, we prepare a dockerfile at the same level of hierarchy as the train.py script in our Python Project.
We then build the image locally using this dockerfile and then push it to ECR following this script. 

The approach is different following if you're working on SageMaker Studio or not. 

#### If you run this Notebook on SageMaker studio : 
Please install the sagemaker-studio-image-build librairy by running the following cell. It will help to build and push your docker image to ECR. 

```!pip install sagemaker-studio-image-build```

```!sh build_and_push_studio.sh```

