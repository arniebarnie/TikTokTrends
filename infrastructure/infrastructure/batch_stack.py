from aws_cdk import (
    Stack,
    aws_batch as batch,
    aws_iam as iam,
    aws_ec2 as ec2,
    Duration,
    CfnOutput,
    Fn
)
from constructs import Construct

class BatchStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, 
                vpc, security_group, container_stack, secrets_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create IAM roles
        self.batch_service_role = iam.Role(self, "BatchServiceRole",
            assumed_by = iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSBatchServiceRole")
            ]
        )

        # Create instance role for GPU instances
        self.instance_role = iam.Role(self, "BatchInstanceRole",
            assumed_by = iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2ContainerServiceforEC2Role")
            ]
        )

        # Add EBS and S3 permissions
        self.instance_role.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "ec2:AttachVolume",
                "ec2:CreateVolume",
                "ec2:DeleteVolume",
                "ec2:DescribeVolumes",
                "ec2:DetachVolume",
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources = ["*"]
        ))

        # Create instance profile
        self.instance_profile = iam.CfnInstanceProfile(self, "BatchInstanceProfile",
            roles = [self.instance_role.role_name]
        )

        # Create launch template for GPU instances
        self.launch_template = ec2.CfnLaunchTemplate(self, "BatchLaunchTemplate",
            launch_template_data = {
                "blockDeviceMappings": [{
                    "deviceName": "/dev/xvda",
                    "ebs": {
                        "volumeSize": 100,
                        "volumeType": "gp3",
                        "deleteOnTermination": True
                    }
                }],
                "userData": Fn.base64(
                    "MIME-Version: 1.0\n"
                    "Content-Type: multipart/mixed; boundary=\"==BOUNDARY==\"\n"
                    "\n"
                    "--==BOUNDARY==\n"
                    "Content-Type: text/cloud-config; charset=\"us-ascii\"\n"
                    "\n"
                    "#cloud-config\n"
                    "repo_update: true\n"
                    "repo_upgrade: all\n"
                    "\n"
                    "--==BOUNDARY==\n"
                    "Content-Type: text/x-shellscript; charset=\"us-ascii\"\n"
                    "\n"
                    "#!/bin/bash\n"
                    "mkdir -p /tmp/workspace\n"
                    "chmod 777 /tmp/workspace\n"
                    "\n"
                    "--==BOUNDARY==--\n"
                )
            }
        )

        # Create Fargate execution role
        self.fargate_execution_role = iam.Role(self, "FargateExecutionRole",
            assumed_by = iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        # Create Fargate task role
        self.fargate_task_role = iam.Role(self, "FargateTaskRole",
            assumed_by = iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )

        # Add S3 permissions to task role
        self.fargate_task_role.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources = [
                "arn:aws:s3:::tiktoktrends",
                "arn:aws:s3:::tiktoktrends/*"
            ]
        ))

        # Create compute environments
        self.fargate_compute_env = batch.CfnComputeEnvironment(self, "FargateComputeEnv",
            type = "MANAGED",
            compute_resources = {
                "type": "FARGATE",
                "maxvCpus": 8,
                "subnets": vpc.private_subnets,
                "securityGroupIds": [security_group.security_group_id],
            },
            service_role = self.batch_service_role.role_arn,
            state = "ENABLED"
        )

        self.gpu_compute_env = batch.CfnComputeEnvironment(self, "GPUComputeEnv",
            type = "MANAGED",
            compute_resources = {
                "type": "SPOT",
                "maxvCpus": 4 * 3,
                "minvCpus": 0,
                "desiredvCpus": 0,
                "instanceTypes": ["g4dn.xlarge"],
                "subnets": vpc.public_subnets,
                "instanceRole": self.instance_profile.attr_arn,
                "securityGroupIds": [security_group.security_group_id],
                "allocationStrategy": "SPOT_CAPACITY_OPTIMIZED",
                "launchTemplate": {
                    "launchTemplateId": self.launch_template.ref,
                    "version": "$Latest"
                },
                "spotIamFleetRole": iam.Role(self, "SpotFleetRole",
                    assumed_by = iam.ServicePrincipal("spotfleet.amazonaws.com"),
                    managed_policies = [
                        iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2SpotFleetTaggingRole")
                    ]
                ).role_arn
            },
            service_role = self.batch_service_role.role_arn,
            state = "ENABLED"
        )

        # Create job queues
        self.fargate_queue = batch.CfnJobQueue(self, "FargateJobQueue",
            compute_environment_order = [{
                "computeEnvironment": self.fargate_compute_env.ref,
                "order": 1
            }],
            priority = 1,
            job_queue_name = "tiktok-fargate-queue"
        )

        self.gpu_queue = batch.CfnJobQueue(self, "GPUJobQueue",
            compute_environment_order = [{
                "computeEnvironment": self.gpu_compute_env.ref,
                "order": 1
            }],
            priority = 1,
            job_queue_name = "tiktok-gpu-queue"
        )

        # Create job definitions
        self.metadata_job = batch.CfnJobDefinition(self, "MetadataJobDefinition",
            type = "container",
            platform_capabilities = ["FARGATE"],
            container_properties = {
                "image": container_stack.metadata_repo_uri,
                "command": ["python3", "main.py"],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "2"},
                    {"type": "MEMORY", "value": "4096"}
                ],
                "executionRoleArn": self.fargate_execution_role.role_arn,
                "jobRoleArn": self.fargate_task_role.role_arn,
                "networkConfiguration": {
                    "assignPublicIp": "ENABLED"
                }
            },
            job_definition_name = "tiktok-metadata-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 6
            }
        )

        self.transcription_job = batch.CfnJobDefinition(self, "TranscriptionJobDefinition",
            type = "container",
            container_properties = {
                "image": container_stack.transcriber_repo_uri,
                "command": ["python3", "main.py"],
                "resourceRequirements": [
                    {"type": "GPU", "value": "1"},
                    {"type": "VCPU", "value": "4"},
                    {"type": "MEMORY", "value": "8000"}
                ],
                "mountPoints": [{
                    "sourceVolume": "workspace",
                    "containerPath": "/workspace",
                    "readOnly": False
                }],
                "volumes": [{
                    "name": "workspace",
                    "host": {
                        "sourcePath": "/tmp/workspace"
                    }
                }]
            },
            job_definition_name = "tiktok-transcriber-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 24
            }
        )

        self.text_analysis_job = batch.CfnJobDefinition(self, "TextAnalysisJobDefinition",
            type = "container",
            platform_capabilities = ["FARGATE"],
            container_properties = {
                "image": container_stack.text_analysis_repo_uri,
                "command": ["python3", "main.py"],
                "environment": [{
                    "name": "OPENAI_SECRET_ARN",
                    "value": secrets_stack.openai_secret_arn
                }],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "2"},
                    {"type": "MEMORY", "value": "4096"}
                ],
                "executionRoleArn": self.fargate_execution_role.role_arn,
                "jobRoleArn": self.fargate_task_role.role_arn,
                "networkConfiguration": {
                    "assignPublicIp": "ENABLED"
                }
            },
            job_definition_name = "tiktok-text-analysis-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 2
            }
        )

        # Add permissions to read OpenAI secret
        self.fargate_task_role.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["secretsmanager:GetSecretValue"],
            resources = [secrets_stack.openai_secret_arn]
        ))

        # Outputs
        CfnOutput(self, "FargateQueueName",
            value = self.fargate_queue.job_queue_name,
            description = "The name of the Fargate job queue",
            export_name = "TiktokFargateQueueName"
        )

        CfnOutput(self, "GPUQueueName",
            value = self.gpu_queue.job_queue_name,
            description = "The name of the GPU job queue",
            export_name = "TiktokGPUQueueName"
        ) 