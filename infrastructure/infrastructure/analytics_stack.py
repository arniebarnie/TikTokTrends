from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_batch as batch,
    Duration,
    CfnOutput,
    RemovalPolicy,
    Fn
)
from constructs import Construct

class AnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC
        self.vpc = ec2.Vpc(self, "TikTokAnalyticsVPC",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                )
            ]
        )

        # Create ECR Repository
        repository = ecr.Repository(self, "TiktokTranscriberRepo",
            repository_name="tiktok-transcriber",
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create IAM role for Batch
        batch_service_role = iam.Role(self, "BatchServiceRole",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com")
        )
        batch_service_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSBatchServiceRole")
        )

        # Create IAM role for container instances
        instance_role = iam.Role(self, "BatchInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com")
        )
        instance_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2ContainerServiceforEC2Role")
        )

        # Add EBS permissions
        instance_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ec2:AttachVolume",
                "ec2:CreateVolume",
                "ec2:DeleteVolume",
                "ec2:DescribeVolumes",
                "ec2:DescribeVolumeStatus",
                "ec2:DetachVolume",
                "ec2:ModifyVolume"
            ],
            resources=["*"]
        ))

        # S3 permissions
        instance_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                "arn:aws:s3:::tiktoktrends",
                "arn:aws:s3:::tiktoktrends/*"
            ]
        ))

        # Create instance profile
        instance_profile = iam.CfnInstanceProfile(self, "BatchInstanceProfile",
            roles=[instance_role.role_name]
        )

        # Create security group for compute environment
        security_group = ec2.SecurityGroup(self, "BatchSecurityGroup",
            vpc=self.vpc,
            description="Security group for AWS Batch Compute Environment",
            allow_all_outbound=True
        )

        # Create Batch compute environment
        compute_environment = batch.CfnComputeEnvironment(self, "BatchComputeEnv",
            type="MANAGED",
            compute_resources={
                "type": "SPOT",
                "maxvCpus": 8,
                "minvCpus": 0,
                "desiredvCpus": 0,
                "instanceTypes": ["g4dn.xlarge"],
                "subnets": [subnet.subnet_id for subnet in self.vpc.public_subnets],
                "instanceRole": instance_profile.attr_arn,
                "securityGroupIds": [security_group.security_group_id],
                "allocationStrategy": "SPOT_CAPACITY_OPTIMIZED",
                "launchTemplate": {
                    "launchTemplateId": ec2.CfnLaunchTemplate(self, "BatchLaunchTemplate",
                        launch_template_data={
                            "blockDeviceMappings": [
                                {
                                    "deviceName": "/dev/xvda",
                                    "ebs": {
                                        "volumeSize": 100,
                                        "volumeType": "gp3",
                                        "deleteOnTermination": True
                                    }
                                }
                            ],
                            "userData": Fn.base64(
                                "#!/bin/bash\n"
                                "mkdir -p /tmp/workspace\n"
                                "chmod 777 /tmp/workspace\n"
                            )
                        }
                    ).ref,
                    "version": "$Latest"
                },
                "spotIamFleetRole": iam.Role(self, "SpotFleetRole",
                    assumed_by=iam.ServicePrincipal("spotfleet.amazonaws.com"),
                    managed_policies=[
                        iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2SpotFleetTaggingRole")
                    ]
                ).role_arn
            },
            service_role=batch_service_role.role_arn,
            state="ENABLED"
        )

        # Create job queue
        job_queue = batch.CfnJobQueue(self, "BatchJobQueue",
            compute_environment_order=[{
                "computeEnvironment": compute_environment.ref,
                "order": 1
            }],
            priority=1,
            job_queue_name="tiktok-transcriber-queue"
        )

        # Create job definition
        job_definition = batch.CfnJobDefinition(self, "BatchJobDefinition",
            type="container",
            container_properties={
                "image": f"{repository.repository_uri}:latest",
                "command": ["python3", "main.py"],
                "environment": [],
                "resourceRequirements": [
                    {"type": "GPU", "value": "1"},
                    {"type": "VCPU", "value": "4"},
                    {"type": "MEMORY", "value": "8000"}
                ],
                "mountPoints": [
                    {
                        "sourceVolume": "workspace",
                        "containerPath": "/workspace",
                        "readOnly": False
                    }
                ],
                "volumes": [
                    {
                        "name": "workspace",
                        "host": {
                            "sourcePath": "/tmp/workspace"
                        }
                    }
                ],
            },
            job_definition_name="tiktok-transcriber-job",
            timeout={
                "attemptDurationSeconds": 7200
            }
        )

        # Add outputs
        CfnOutput(self, "JobQueueName",
            value=job_queue.job_queue_name,
            description="The name of the job queue"
        )

        CfnOutput(self, "JobDefinitionName",
            value=job_definition.job_definition_name,
            description="The name of the job definition"
        )

        CfnOutput(self, "RepositoryUri",
            value=repository.repository_uri,
            description="The URI of the ECR repository"
        ) 