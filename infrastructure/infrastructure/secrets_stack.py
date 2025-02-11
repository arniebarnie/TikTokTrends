from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    CfnOutput,
    RemovalPolicy
)
from constructs import Construct

class SecretsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create secret for OpenAI API key
        self.openai_secret = secretsmanager.Secret(self, "OpenAISecret",
            secret_name = "tiktok/openai-api-key",
            description = "OpenAI API Key for TikTok text analysis",
            removal_policy = RemovalPolicy.RETAIN  # Protect against accidental deletion
        )

        # Create a role that can read the OpenAI secret
        self.secret_reader_role = iam.Role(self, "SecretReaderRole",
            assumed_by = iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description = "Role that can read the OpenAI API key secret"
        )

        # Grant read permission to the role
        self.openai_secret.grant_read(self.secret_reader_role)

        # Outputs
        CfnOutput(self, "OpenAISecretArn",
            value = self.openai_secret.secret_arn,
            description = "The ARN of the OpenAI API key secret",
            export_name = "OpenAISecretArn"  # Allow other stacks to import
        )

        CfnOutput(self, "SecretReaderRoleArn",
            value = self.secret_reader_role.role_arn,
            description = "The ARN of the role that can read secrets",
            export_name = "SecretReaderRoleArn"
        )

    @property
    def openai_secret_arn(self) -> str:
        """Get the ARN of the OpenAI secret"""
        return self.openai_secret.secret_arn

    @property
    def secret_reader_role_arn(self) -> str:
        """Get the ARN of the secret reader role"""
        return self.secret_reader_role.role_arn 