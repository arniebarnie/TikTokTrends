from aws_cdk import Stack
from constructs import Construct
from .network_stack import NetworkStack
from .storage_stack import StorageStack
from .container_stack import ContainerStack
from .secrets_stack import SecretsStack
from .batch_stack import BatchStack
from .serverless_stack import ServerlessStack

class AnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, storage_stack: StorageStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create stacks in dependency order
        self.secrets_stack = SecretsStack(self, "SecretsStack")
        
        # Use provided storage stack
        self.storage_stack = storage_stack
        
        self.network_stack = NetworkStack(self, "NetworkStack")
        
        self.container_stack = ContainerStack(self, "ContainerStack")
        
        self.batch_stack = BatchStack(self, "BatchStack",
            vpc = self.network_stack.vpc,
            security_group = self.network_stack.batch_security_group,
            container_stack = self.container_stack,
            secrets_stack = self.secrets_stack
        )
        
        self.serverless_stack = ServerlessStack(self, "ServerlessStack",
            storage_stack = self.storage_stack,
            batch_stack = self.batch_stack
        )

    @property
    def storage(self) -> StorageStack:
        """Get the storage stack"""
        return self.storage_stack 