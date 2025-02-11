#!/usr/bin/env python3
import os
import aws_cdk as cdk
from infrastructure.analytics_stack import AnalyticsStack
from infrastructure.dashboard_stack import DashboardStack
from infrastructure.storage_stack import StorageStack

app = cdk.App()

env = cdk.Environment(
    account = os.environ["CDK_DEFAULT_ACCOUNT"],
    region = os.environ["CDK_DEFAULT_REGION"]
)

# Create shared storage stack first
storage_stack = StorageStack(app, "TiktokStorageStack", env = env)

# Create analytics stack with storage
analytics_stack = AnalyticsStack(app, "TiktokAnalyticsStack", 
    storage_stack = storage_stack,
    env = env
)

# Create dashboard stack with storage
dashboard_stack = DashboardStack(app, "TiktokDashboardStack", 
    storage_stack = storage_stack,
    env = env
)

# Add dependencies on storage
analytics_stack.add_dependency(storage_stack)
dashboard_stack.add_dependency(storage_stack)

app.synth()