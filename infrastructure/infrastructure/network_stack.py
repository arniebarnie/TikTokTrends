from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    CfnOutput
)
from constructs import Construct

class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC
        self.vpc = ec2.Vpc(self, "TiktokVPC",
            max_azs = 2,
            nat_gateways = 1,
            subnet_configuration = [
                ec2.SubnetConfiguration(
                    name = "Public",
                    subnet_type = ec2.SubnetType.PUBLIC,
                    cidr_mask = 24
                ),
                ec2.SubnetConfiguration(
                    name = "Private",
                    subnet_type = ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask = 24
                )
            ]
        )

        # Create security group for compute environments
        self.batch_security_group = ec2.SecurityGroup(self, "BatchSecurityGroup",
            vpc = self.vpc,
            description = "Security group for AWS Batch Compute Environment",
            allow_all_outbound = True
        )

        # Outputs
        CfnOutput(self, "VpcId",
            value = self.vpc.vpc_id,
            description = "The ID of the VPC",
            export_name = "TiktokVpcId"
        )

        CfnOutput(self, "BatchSecurityGroupId",
            value = self.batch_security_group.security_group_id,
            description = "The ID of the Batch security group",
            export_name = "TiktokBatchSecurityGroupId"
        )

    @property
    def vpc_id(self) -> str:
        """Get the VPC ID"""
        return self.vpc.vpc_id

    @property
    def private_subnets(self) -> list:
        """Get the private subnet IDs"""
        return [subnet.subnet_id for subnet in self.vpc.private_subnets]

    @property
    def public_subnets(self) -> list:
        """Get the public subnet IDs"""
        return [subnet.subnet_id for subnet in self.vpc.public_subnets]

    @property
    def batch_sg_id(self) -> str:
        """Get the Batch security group ID"""
        return self.batch_security_group.security_group_id 