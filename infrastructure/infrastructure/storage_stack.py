from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_glue as glue,
    RemovalPolicy,
    CfnOutput,
    Duration
)
from constructs import Construct

class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, bucket_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket with provided name
        self.bucket = s3.Bucket(self, "TiktokBucket",
            bucket_name = bucket_name,
            removal_policy = RemovalPolicy.RETAIN,
        )

        # Create SNS topics for different events
        self.metadata_topic = sns.Topic(self, "MetadataTopic",
            display_name = "Metadata Processing Topic"
        )

        self.transcript_topic = sns.Topic(self, "TranscriptTopic",
            display_name = "Transcript Processing Topic"
        )

        self.text_topic = sns.Topic(self, "TextTopic",
            display_name = "Text Analysis Topic"
        )

        # Add S3 notifications to SNS topics
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(self.metadata_topic),
            s3.NotificationKeyFilter(prefix="videos/metadata/")
        )

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(self.transcript_topic),
            s3.NotificationKeyFilter(prefix="videos/transcripts/")
        )

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(self.text_topic),
            s3.NotificationKeyFilter(prefix="videos/text/")
        )

        # Create Glue Database
        self.database = glue.CfnDatabase(self, "TiktokAnalyticsDB",
            catalog_id = Stack.of(self).account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name = "tiktok_analytics",
                description = "Database for TikTok video analytics"
            )
        )

        # Create Metadata Table
        self.metadata_table = glue.CfnTable(self, "TiktokMetadataTable",
            catalog_id = Stack.of(self).account,
            database_name = self.database.ref,
            table_input = glue.CfnTable.TableInputProperty(
                name = "metadata",
                description = "TikTok video metadata",
                parameters = {
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                    "EXTERNAL": "TRUE"
                },
                storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
                    location = f"s3://{self.bucket.bucket_name}/videos/metadata",
                    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    columns = [
                        glue.CfnTable.ColumnProperty(name = "id", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "title", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "description", type = "string"),
                        glue.CfnTable.ColumnProperty(name = "upload_date", type = "timestamp"),
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
                        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                partition_keys = [
                    glue.CfnTable.ColumnProperty(name = "profile", type = "string"),
                    glue.CfnTable.ColumnProperty(name = "processed_at", type = "string")
                ]
            )
        )

        # Create Text Analysis Table
        self.text_table = glue.CfnTable(self, "TiktokTextTable",
            catalog_id = Stack.of(self).account,
            database_name = self.database.ref,
            table_input = glue.CfnTable.TableInputProperty(
                name = "text_analysis",
                description = "TikTok video text analysis",
                parameters = {
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                    "EXTERNAL": "TRUE"
                },
                storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
                    location = f"s3://{self.bucket.bucket_name}/videos/text",
                    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    columns = [
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
                        serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                partition_keys = [
                    glue.CfnTable.ColumnProperty(name = "profile", type = "string"),
                    glue.CfnTable.ColumnProperty(name = "processed_at", type = "string")
                ]
            )
        )

        # Add dependencies
        self.metadata_table.add_dependency(self.database)
        self.text_table.add_dependency(self.database)

        # Outputs
        CfnOutput(self, "BucketName",
            value = self.bucket.bucket_name,
            description = "The name of the S3 bucket",
            export_name = "TiktokBucketName"
        )

        CfnOutput(self, "DatabaseName",
            value = self.database.ref,
            description = "The name of the Glue database",
            export_name = "TiktokDatabaseName"
        )

        # Add topic outputs
        CfnOutput(self, "MetadataTopicArn",
            value = self.metadata_topic.topic_arn,
            description = "ARN of the metadata processing topic",
            export_name = "TiktokMetadataTopicArn"
        )

        CfnOutput(self, "TranscriptTopicArn",
            value = self.transcript_topic.topic_arn,
            description = "ARN of the transcript processing topic",
            export_name = "TiktokTranscriptTopicArn"
        )

        CfnOutput(self, "TextTopicArn",
            value = self.text_topic.topic_arn,
            description = "ARN of the text analysis topic",
            export_name = "TiktokTextTopicArn"
        )

    @property
    def bucket_name(self) -> str:
        """Get the name of the S3 bucket"""
        return self.bucket.bucket_name

    @property
    def database_name(self) -> str:
        """Get the name of the Glue database"""
        return self.database.ref

    @property
    def bucket_arn(self) -> str:
        """Get the ARN of the S3 bucket"""
        return self.bucket.bucket_arn

    @property
    def metadata_topic_arn(self) -> str:
        return self.metadata_topic.topic_arn

    @property
    def transcript_topic_arn(self) -> str:
        return self.transcript_topic.topic_arn

    @property
    def text_topic_arn(self) -> str:
        return self.text_topic.topic_arn 