#!/usr/bin/env python3
import os
import aws_cdk as cdk
from infrastructure.analytics_stack import AnalyticsStack
from infrastructure.dashboard_stack import DashboardStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]
)

analytics_stack = AnalyticsStack(app, "TiktokAnalyticsStack", env=env)
dashboard_stack = DashboardStack(app, "TiktokDashboardStack", 
    vpc=analytics_stack.vpc,  # Pass VPC from analytics stack
    env=env
)

# Add dependency
dashboard_stack.add_dependency(analytics_stack)

app.synth()