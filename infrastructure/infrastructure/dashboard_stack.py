from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_glue as glue,
    aws_secretsmanager as secretsmanager,
    aws_ssm as ssm,
    aws_lambda as lambda_,
    Duration,
    CfnOutput
)
from constructs import Construct
from .storage_stack import StorageStack

class DashboardStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Use the provided storage stack
        self.storage_stack = storage_stack