from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    RemovalPolicy,
    CfnOutput,
    Duration
)
from constructs import Construct

class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket
        self.bucket = s3.Bucket(self, "TiktokBucket",
            bucket_name = "tiktoktrends",
            removal_policy = RemovalPolicy.RETAIN,
            versioned = True,
            lifecycle_rules = [
                s3.LifecycleRule(
                    transitions = [
                        s3.Transition(
                            storage_class = s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after = Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class = s3.StorageClass.GLACIER,
                            transition_after = Duration.days(90)
                        )
                    ]
                )
            ]
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