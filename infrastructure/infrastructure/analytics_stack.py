from aws_cdk import Stack
from constructs import Construct
from .network_stack import NetworkStack
from .storage_stack import StorageStack
from .container_stack import ContainerStack
from .secrets_stack import SecretsStack
from .batch_stack import BatchStack
from .serverless_stack import ServerlessStack

class AnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, 
                 storage_stack: StorageStack,
                 secrets_stack: SecretsStack,
                 network_stack: NetworkStack,
                 container_stack: ContainerStack,
                 batch_stack: BatchStack,
                 serverless_stack: ServerlessStack,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Use provided stacks
        self.secrets_stack = secrets_stack
        self.storage_stack = storage_stack
        self.network_stack = network_stack
        self.container_stack = container_stack
        self.batch_stack = batch_stack
        self.serverless_stack = serverless_stack

    @property
    def storage(self) -> StorageStack:
        """Get the storage stack"""
        return self.storage_stack 