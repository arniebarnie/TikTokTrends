from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_iam as iam,
    Duration,
    CfnOutput
)
from constructs import Construct

class ServerlessStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, 
                storage_stack, batch_stack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Common environment variables
        common_env = {
            "ATHENA_RESULTS_BUCKET": storage_stack.bucket_name,
            "S3_BUCKET": storage_stack.bucket_name
        }

        # Create Lambda function for metadata trigger
        self.metadata_trigger = lambda_.Function(self, "MetadataTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/metadata_trigger"),
            environment = {
                **common_env,
                "GPU_JOB_QUEUE": batch_stack.gpu_queue.job_queue_name,
                "TRANSCRIBER_JOB_DEFINITION": batch_stack.transcription_job.job_definition_name,
            },
            timeout = Duration.minutes(1),
            function_name = "tiktok-metadata-trigger"
        )

        # Add SQS trigger for metadata function
        self.metadata_trigger.add_event_source(lambda_events.SqsEventSource(
            storage_stack.metadata_queue,
            batch_size = 1
        ))

        # Create Lambda function for transcript trigger
        self.transcript_trigger = lambda_.Function(self, "TranscriptTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/transcript_trigger"),
            environment = {
                **common_env,
                "FARGATE_JOB_QUEUE": batch_stack.fargate_text_queue.job_queue_name,
                "TEXT_ANALYSIS_JOB_DEFINITION": batch_stack.text_analysis_job.job_definition_name,
            },
            timeout = Duration.minutes(1),
            function_name = "tiktok-transcript-trigger"
        )

        # Add SQS trigger for transcript function
        self.transcript_trigger.add_event_source(lambda_events.SqsEventSource(
            storage_stack.transcript_queue,
            batch_size = 1
        ))

        # Create Lambda function for text trigger
        self.text_trigger = lambda_.Function(self, "TextTriggerFunction",
            runtime = lambda_.Runtime.PYTHON_3_9,
            handler = "index.handler",
            code = lambda_.Code.from_asset("infrastructure/lambda/text_trigger"),
            environment = common_env,
            timeout = Duration.minutes(5),
            function_name = "tiktok-text-trigger"
        )

        # Add SQS trigger for text function
        self.text_trigger.add_event_source(lambda_events.SqsEventSource(
            storage_stack.text_queue,
            batch_size = 1
        ))

        # Add permissions for all functions
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

        # Add specific permissions for metadata trigger
        self.metadata_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = [
                "batch:SubmitJob",
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
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{batch_stack.gpu_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{batch_stack.transcription_job.job_definition_name}:*",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/tiktok_analytics",
                f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/tiktok_analytics/*",
                "*"  # For Athena permissions
            ]
        ))

        # Add specific permissions for transcript trigger
        self.transcript_trigger.add_to_role_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            actions = ["batch:SubmitJob"],
            resources = [
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-queue/{batch_stack.fargate_text_queue.job_queue_name}",
                f"arn:aws:batch:{Stack.of(self).region}:{Stack.of(self).account}:job-definition/{batch_stack.text_analysis_job.job_definition_name}:*"
            ]
        ))

        # Add specific permissions for text trigger
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