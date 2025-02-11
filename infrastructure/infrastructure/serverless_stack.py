from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_s3_notifications as s3n,
    aws_s3 as s3,
    aws_iam as iam,
    Duration,
    CfnOutput
)
from constructs import Construct

class ServerlessStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, 
                storage_stack, batch_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Lambda function for metadata trigger
        self.metadata_trigger = lambda_.Function(self, "MetadataTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/metadata_trigger"),
            environment = {
                "GPU_JOB_QUEUE": batch_stack.gpu_queue.job_queue_name,
                "TRANSCRIBER_JOB_DEFINITION": batch_stack.transcription_job.job_definition_name,
            },
            timeout = Duration.minutes(1),
            function_name = "tiktok-metadata-trigger"
        )

        # Add permissions for metadata trigger
        self.metadata_trigger.add_to_role_policy(iam.PolicyStatement(
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
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{batch_stack.gpu_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{batch_stack.transcription_job.job_definition_name}:*",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/tiktok_analytics",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/tiktok_analytics/*",
                "*"  # For Athena permissions
            ]
        ))

        # Create Lambda function for transcript trigger
        self.transcript_trigger = lambda_.Function(self, "TranscriptTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/transcript_trigger"),
            environment = {
                "FARGATE_JOB_QUEUE": batch_stack.fargate_queue.job_queue_name,
                "TEXT_ANALYSIS_JOB_DEFINITION": batch_stack.text_analysis_job.job_definition_name,
            },
            timeout = Duration.minutes(1),
            function_name = "tiktok-transcript-trigger"
        )

        # Add permissions for transcript trigger
        self.transcript_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["batch:SubmitJob"],
            resources = [
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{batch_stack.fargate_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{batch_stack.text_analysis_job.job_definition_name}:*"
            ]
        ))

        # Create Lambda function for text trigger (partition management)
        self.text_trigger = lambda_.Function(self, "TextTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/text_trigger"),
            timeout = Duration.minutes(5),
            function_name = "tiktok-text-trigger"
        )

        # Add permissions for text trigger
        self.text_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetWorkGroup",
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

        # Add S3 permissions for all functions
        for function in [self.metadata_trigger, self.transcript_trigger, self.text_trigger]:
            function.add_to_role_policy(iam.PolicyStatement(
                effect = iam.Effect.ALLOW,
                actions = [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                resources = [
                    storage_stack.bucket_arn,
                    f"{storage_stack.bucket_arn}/*"
                ]
            ))

        # Add S3 triggers
        storage_stack.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.metadata_trigger),
            s3.NotificationKeyFilter(prefix="videos/metadata/")
        )

        storage_stack.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.transcript_trigger),
            s3.NotificationKeyFilter(prefix="videos/transcripts/")
        )

        storage_stack.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.text_trigger),
            s3.NotificationKeyFilter(prefix="videos/text/")
        )

        # Outputs
        CfnOutput(self, "MetadataTriggerArn",
            value = self.metadata_trigger.function_arn,
            description = "The ARN of the metadata trigger function",
            export_name = "TiktokMetadataTriggerArn"
        )

        CfnOutput(self, "TranscriptTriggerArn",
            value = self.transcript_trigger.function_arn,
            description = "The ARN of the transcript trigger function",
            export_name = "TiktokTranscriptTriggerArn"
        )

        CfnOutput(self, "TextTriggerArn",
            value = self.text_trigger.function_arn,
            description = "The ARN of the text trigger function",
            export_name = "TiktokTextTriggerArn"
        ) 