#!/usr/bin/env python3
import os
import aws_cdk as cdk
from infrastructure.analytics_stack import AnalyticsStack
from infrastructure.dashboard_stack import DashboardStack
from infrastructure.storage_stack import StorageStack
from infrastructure.secrets_stack import SecretsStack
from infrastructure.network_stack import NetworkStack
from infrastructure.container_stack import ContainerStack
from infrastructure.batch_stack import BatchStack
from infrastructure.serverless_stack import ServerlessStack

app = cdk.App()

# Define constants
BUCKET_NAME = "tiktoktrends-3"

env = cdk.Environment(
    account = os.environ["CDK_DEFAULT_ACCOUNT"],
    region = os.environ["CDK_DEFAULT_REGION"]
)

# Create shared infrastructure stacks first
storage_stack = StorageStack(app, "TiktokStorageStack", 
    bucket_name = BUCKET_NAME,  # Pass bucket name as parameter
    env = env
)
secrets_stack = SecretsStack(app, "TiktokSecretsStack", env = env)
network_stack = NetworkStack(app, "TiktokNetworkStack", env = env)
container_stack = ContainerStack(app, "TiktokContainerStack", env = env)
batch_stack = BatchStack(app, "TiktokBatchStack",
    vpc = network_stack.vpc,
    security_group = network_stack.batch_security_group,
    container_stack = container_stack,
    secrets_stack = secrets_stack,
    storage_stack = storage_stack,
    env = env
)
serverless_stack = ServerlessStack(app, "TiktokServerlessStack",
    storage_stack = storage_stack,
    batch_stack = batch_stack,
    env = env
)

# Create analytics stack with dependencies
analytics_stack = AnalyticsStack(app, "TiktokAnalyticsStack", 
    storage_stack = storage_stack,
    secrets_stack = secrets_stack,
    network_stack = network_stack,
    container_stack = container_stack,
    batch_stack = batch_stack,
    serverless_stack = serverless_stack,
    env = env
)

# Create dashboard stack with storage
dashboard_stack = DashboardStack(app, "TiktokDashboardStack", 
    storage_stack = storage_stack,
    env = env
)

# Batch stack dependencies
batch_stack.add_dependency(network_stack)
batch_stack.add_dependency(container_stack)
batch_stack.add_dependency(secrets_stack)

# Serverless stack dependencies
serverless_stack.add_dependency(storage_stack)
serverless_stack.add_dependency(batch_stack)

# Analytics stack dependencies
analytics_stack.add_dependency(storage_stack)
analytics_stack.add_dependency(secrets_stack)
analytics_stack.add_dependency(network_stack)
analytics_stack.add_dependency(container_stack)
analytics_stack.add_dependency(batch_stack)
analytics_stack.add_dependency(serverless_stack)

# Dashboard stack dependencies
dashboard_stack.add_dependency(storage_stack)

app.synth()