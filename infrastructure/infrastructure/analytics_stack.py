from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_batch as batch,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    Duration,
    CfnOutput,
    RemovalPolicy,
    Fn,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue
)
from constructs import Construct
from aws_cdk.aws_lambda import Function as LambdaFunction
from aws_cdk.aws_s3 import Bucket as S3Bucket
import boto3

class AnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC
        self.vpc = ec2.Vpc(self, "TikTokAnalyticsVPC",
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

        # Create ECR Repositories
        metadata_repository = ecr.Repository(self, "TiktokMetadataRepo",
            repository_name = "tiktok-metadata",
            removal_policy = RemovalPolicy.DESTROY
        )

        transcription_repository = ecr.Repository(self, "TiktokTranscriberRepo",
            repository_name = "tiktok-transcriber",
            removal_policy = RemovalPolicy.DESTROY
        )

        text_analysis_repository = ecr.Repository(self, "TiktokTextAnalysisRepo",
            repository_name = "tiktok-text-analysis",
            removal_policy = RemovalPolicy.DESTROY
        )

        # Create IAM role for Batch
        self.batch_service_role = iam.Role(self, "BatchServiceRole",
            assumed_by = iam.ServicePrincipal("batch.amazonaws.com")
        )
        self.batch_service_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSBatchServiceRole")
        )

        # Create IAM role for container instances
        instance_role = iam.Role(self, "BatchInstanceRole",
            assumed_by = iam.ServicePrincipal("ec2.amazonaws.com")
        )
        instance_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2ContainerServiceforEC2Role")
        )

        # Add EBS permissions
        instance_role.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "ec2:AttachVolume",
                "ec2:CreateVolume",
                "ec2:DeleteVolume",
                "ec2:DescribeVolumes",
                "ec2:DescribeVolumeStatus",
                "ec2:DetachVolume",
                "ec2:ModifyVolume"
            ],
            resources = ["*"]
        ))

        # S3 permissions
        instance_role.add_to_policy(iam.PolicyStatement(
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

        # Create instance profile
        instance_profile = iam.CfnInstanceProfile(self, "BatchInstanceProfile",
            roles = [instance_role.role_name]
        )

        # Create security group for compute environment
        self.security_group = ec2.SecurityGroup(self, "BatchSecurityGroup",
            vpc = self.vpc,
            description = "Security group for AWS Batch Compute Environment",
            allow_all_outbound = True
        )

        # Create launch template first
        launch_template = ec2.CfnLaunchTemplate(self, "BatchLaunchTemplate",
            launch_template_data = {
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
        fargate_execution_role = iam.Role(self, "FargateExecutionRole",
            assumed_by = iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        # Create Fargate task role
        fargate_task_role = iam.Role(self, "FargateTaskRole",
            assumed_by = iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )

        # Add S3 permissions to task role
        fargate_task_role.add_to_policy(iam.PolicyStatement(
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

        # Create Fargate job queue
        fargate_job_queue = batch.CfnJobQueue(self, "FargateJobQueue",
            compute_environment_order = [{
                "computeEnvironment": self.create_fargate_compute_environment().ref,
                "order": 1
            }],
            priority = 1,
            job_queue_name = "tiktok-fargate-queue"
        )

        # Create GPU job queue
        gpu_job_queue = batch.CfnJobQueue(self, "GPUJobQueue",
            compute_environment_order = [{
                "computeEnvironment": self.create_gpu_compute_environment(
                    instance_profile, self.security_group, launch_template, self.batch_service_role
                ).ref,
                "order": 1
            }],
            priority = 1,
            job_queue_name = "tiktok-gpu-queue"
        )

        # Create secret for OpenAI API key
        openai_secret = secretsmanager.Secret(self, "OpenAISecret",
            secret_name = "tiktok/openai-api-key",
            description = "OpenAI API Key for TikTok text analysis"
        )

        # Create job definitions
        metadata_job_definition = batch.CfnJobDefinition(self, "MetadataJobDefinition",
            type = "container",
            platform_capabilities = ["FARGATE"],
            container_properties = {
                "image": f"{metadata_repository.repository_uri}:latest",
                "command": ["python3", "main.py"],
                "environment": [],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "2"},
                    {"type": "MEMORY", "value": "4096"}
                ],
                "executionRoleArn": fargate_execution_role.role_arn,
                "jobRoleArn": fargate_task_role.role_arn,
                "networkConfiguration": {
                    "assignPublicIp": "ENABLED"
                }
            },
            job_definition_name = "tiktok-metadata-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 6
            }
        )

        transcription_job_definition = batch.CfnJobDefinition(self, "TranscriptionJobDefinition",
            type = "container",
            container_properties = {
                "image": f"{transcription_repository.repository_uri}:latest",
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
            job_definition_name = "tiktok-transcriber-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 24
            }
        )

        text_analysis_job_definition = batch.CfnJobDefinition(self, "TextAnalysisJobDefinition",
            type = "container",
            platform_capabilities = ["FARGATE"],
            container_properties = {
                "image": f"{text_analysis_repository.repository_uri}:latest",
                "command": ["python3", "main.py"],
                "environment": [
                    {
                        "name": "OPENAI_SECRET_ARN",
                        "value": openai_secret.secret_arn
                    }
                ],
                "resourceRequirements": [
                    {"type": "VCPU", "value": "2"},
                    {"type": "MEMORY", "value": "4096"}
                ],
                "executionRoleArn": fargate_execution_role.role_arn,
                "jobRoleArn": fargate_task_role.role_arn,
                "networkConfiguration": {
                    "assignPublicIp": "ENABLED"
                }
            },
            job_definition_name = "tiktok-text-analysis-job",
            timeout = {
                "attemptDurationSeconds": 60 * 60 * 1
            }
        )

        # Add permissions to text analysis job to read secret
        fargate_task_role.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["secretsmanager:GetSecretValue"],
            resources = [openai_secret.secret_arn]
        ))

        # Create Lambda function for triggering transcription jobs
        metadata_trigger = LambdaFunction(self, "MetadataTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/metadata_trigger"),
            environment = {
                "GPU_JOB_QUEUE": gpu_job_queue.job_queue_name,
                "TRANSCRIBER_JOB_DEFINITION": transcription_job_definition.job_definition_name,
            },
            timeout = Duration.minutes(1)
        )
        
        # Add permissions for Lambda to submit Batch jobs and use Athena/Glue
        metadata_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "batch:SubmitJob",
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetWorkGroup",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchCreatePartition"
            ],
            resources = [
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{gpu_job_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{transcription_job_definition.job_definition_name}:*",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/tiktok_analytics",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/tiktok_analytics/*",
                "*"  # For Athena permissions
            ]
        ))
        
        # Add S3 permissions for Lambda
        metadata_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            resources = [
                "arn:aws:s3:::tiktoktrends",  # Add bucket-level permissions
                "arn:aws:s3:::tiktoktrends/*"
            ]
        ))

        # Add S3 trigger for the Lambda
        bucket = S3Bucket.from_bucket_name(self, "TiktokBucket", "tiktoktrends")
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3n.LambdaDestination(metadata_trigger),
            s3.NotificationKeyFilter(prefix="videos/metadata/")
        )

        # Create Lambda function for triggering text analysis jobs
        transcript_trigger = LambdaFunction(self, "TranscriptTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/transcript_trigger"),
            environment = {
                "FARGATE_JOB_QUEUE": fargate_job_queue.job_queue_name,
                "TEXT_ANALYSIS_JOB_DEFINITION": text_analysis_job_definition.job_definition_name,
            },
            timeout = Duration.minutes(1)
        )
        
        # Add permissions for Lambda to submit Batch jobs
        transcript_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["batch:SubmitJob"],
            resources = [
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{fargate_job_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/tiktok-text-analysis-job",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{text_analysis_job_definition.job_definition_name}:*"
            ]
        ))
        
        # Add S3 read permissions for the Lambda
        transcript_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["s3:GetObject"],
            resources = [
                "arn:aws:s3:::tiktoktrends/*"
            ]
        ))

        # Add S3 trigger for transcript uploads
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3n.LambdaDestination(transcript_trigger),
            s3.NotificationKeyFilter(prefix = "videos/transcripts/")
        )

        # Create Glue Database
        database = glue.CfnDatabase(self, "TiktokAnalyticsDB",
            catalog_id = self.account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name = "tiktok_analytics",
                description = "Database for TikTok video analytics"
            )
        )

        # Create Metadata Table
        metadata_table = glue.CfnTable(self, "TiktokMetadataTable",
            catalog_id = self.account,
            database_name = database.ref,
            table_input = glue.CfnTable.TableInputProperty(
                name = "metadata",
                description = "TikTok video metadata",
                parameters = {
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                    "EXTERNAL": "TRUE",
                    "typeOfData": "file",
                    "input.format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "output.format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serde.serialization.lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
                    location = "s3://tiktoktrends/videos/metadata",
                    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    columns=[
                        glue.CfnTable.ColumnProperty(name = "id", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "title", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "description", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "upload_date", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "like_count", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "repost_count", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "comment_count", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "view_count", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "duration", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "webpage_url", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "channel", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "timestamp", type = "bigint"),
                        glue.CfnTable.ColumnProperty(name = "track", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "artists", type = "array<string>"),
                        glue.CfnTable.ColumnProperty(name = "artist", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "uploader", type = "string")
                    ],
                    serde_info = glue.CfnTable.SerdeInfoProperty(
                        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters = {
                            "serialization.format": "1"
                        }
                    )
                ),
                partition_keys = [
                    glue.CfnTable.ColumnProperty(name = "profile", type = "string"),
                    glue.CfnTable.ColumnProperty(name = "processed_at", type = "string")
                ]
            )
        )

        # Create Text Analysis Table
        text_table = glue.CfnTable(self, "TiktokTextTable",
            catalog_id = self.account,
            database_name = database.ref,
            table_input = glue.CfnTable.TableInputProperty(
                name = "text_analysis",
                description = "TikTok video text analysis",
                parameters = {
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                    "EXTERNAL": "TRUE",
                    "typeOfData": "file",
                    "input.format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "output.format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "serde.serialization.lib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
                    location = "s3://tiktoktrends/videos/text",
                    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    columns=[
                        glue.CfnTable.ColumnProperty(name = "id", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "uploader", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "description", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "title", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "transcript", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "language", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "category", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "summary", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "keywords", type = "array<string>")
                    ],
                    serde_info = glue.CfnTable.SerdeInfoProperty(
                        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters = {
                            "serialization.format": "1"
                        }
                    )
                ),
                partition_keys = [
                    glue.CfnTable.ColumnProperty(name = "profile", type = "string"),
                    glue.CfnTable.ColumnProperty(name = "processed_at", type = "string")
                ]
            )
        )

        # Add dependencies
        metadata_table.add_dependency(database)
        text_table.add_dependency(database)

        # Create text partition handler Lambda
        text_trigger = LambdaFunction(self, "TextTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/text_trigger"),
            timeout = Duration.minutes(5)
        )
        
        # Add Athena/Glue permissions for text trigger
        text_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetWorkGroup",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchCreatePartition"
            ],
            resources = [
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/tiktok_analytics",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/tiktok_analytics/*",
                "*"  # For Athena permissions
            ]
        ))
        
        # Add S3 permissions for text trigger
        text_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            resources = [
                "arn:aws:s3:::tiktoktrends",  # Add bucket-level permissions
                "arn:aws:s3:::tiktoktrends/*"
            ]
        ))
        
        # Add S3 trigger for text analysis files
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3n.LambdaDestination(text_trigger),
            s3.NotificationKeyFilter(
                prefix = "videos/text/",
            )
        )
        
        # Add outputs
        CfnOutput(self, "MetadataRepositoryUri",
            value = metadata_repository.repository_uri,
            description = "The URI of the metadata ECR repository"
        )
        
        CfnOutput(self, "TranscriberRepositoryUri",
            value = transcription_repository.repository_uri,
            description = "The URI of the transcriber ECR repository"
        )
        
        CfnOutput(self, "TextAnalysisRepositoryUri",
            value = text_analysis_repository.repository_uri,
            description = "The URI of the text analysis ECR repository"
        )

        CfnOutput(self, "FargateJobQueueName",
            value = fargate_job_queue.job_queue_name,
            description = "The name of the Fargate job queue"
        )

        CfnOutput(self, "GPUJobQueueName",
            value = gpu_job_queue.job_queue_name,
            description = "The name of the GPU job queue"
        )

        CfnOutput(self, "MetadataJobDefinitionName",
            value = metadata_job_definition.job_definition_name,
            description = "The name of the metadata job definition"
        )

        CfnOutput(self, "TranscriberJobDefinitionName",
            value = transcription_job_definition.job_definition_name,
            description = "The name of the transcriber job definition"
        )

        CfnOutput(self, "TextAnalysisJobDefinitionName",
            value = text_analysis_job_definition.job_definition_name,
            description = "The name of the text analysis job definition"
        )

        CfnOutput(self, "OpenAISecretArn",
            value = openai_secret.secret_arn,
            description = "The ARN of the OpenAI API key secret"
        )

    def create_fargate_compute_environment(self):
        return batch.CfnComputeEnvironment(self, "FargateComputeEnv",
            type = "MANAGED",
            compute_resources = {
                "type": "FARGATE",
                "maxvCpus": 8,
                "subnets": [subnet.subnet_id for subnet in self.vpc.private_subnets],
                "securityGroupIds": [self.security_group.security_group_id],
            },
            service_role = self.batch_service_role.role_arn,
            state = "ENABLED"
        )

    def create_gpu_compute_environment(self, instance_profile, security_group, launch_template, batch_service_role):
        return batch.CfnComputeEnvironment(self, "GPUComputeEnv",
            type="MANAGED",
            compute_resources = {
                "type": "SPOT",
                "maxvCpus": 4 * 3,
                "minvCpus": 0,
                "desiredvCpus": 0,
                "instanceTypes": ["g4dn.xlarge"],
                "subnets": [subnet.subnet_id for subnet in self.vpc.public_subnets],
                "instanceRole": instance_profile.attr_arn,
                "securityGroupIds": [security_group.security_group_id],
                "allocationStrategy": "SPOT_CAPACITY_OPTIMIZED",
                "launchTemplate": {
                    "launchTemplateId": launch_template.ref,
                    "version": "$Latest"
                },
                "spotIamFleetRole": iam.Role(self, "SpotFleetRole",
                    assumed_by = iam.ServicePrincipal("spotfleet.amazonaws.com"),
                    managed_policies = [
                        iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2SpotFleetTaggingRole")
                    ]
                ).role_arn
            },
            service_role = batch_service_role.role_arn,
            state="ENABLED"
        ) 