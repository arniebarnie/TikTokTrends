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
        
        # Create IAM user for Streamlit Cloud
        streamlit_user = iam.User(
            self,
            "StreamlitUser",
            user_name="streamlit-cloud-user"
        )
        
        # Create IAM role that the user can assume
        streamlit_role = iam.Role(
            self,
            "StreamlitRole",
            assumed_by=iam.AccountPrincipal(self.account),
            description="Role for Streamlit Cloud to access Athena"
        )
        
        # Allow the user to assume this role
        streamlit_role.grant_assume_role(streamlit_user)
        
        # Add policies for Athena access
        streamlit_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
        )
        
        # Add policy for S3 access
        streamlit_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:PutObject"
                ],
                resources=[
                    self.storage_stack.bucket.bucket_arn,
                    f"{self.storage_stack.bucket.bucket_arn}/*"
                ]
            )
        )
        
        # Output role ARN
        CfnOutput(
            self,
            "StreamlitRoleArn",
            value=streamlit_role.role_arn,
            description="ARN of the IAM role for Streamlit Cloud"
        )