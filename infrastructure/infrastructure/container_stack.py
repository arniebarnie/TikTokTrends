from aws_cdk import (
    Stack,
    aws_ecr as ecr,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct

class ContainerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create ECR Repositories
        self.metadata_repository = ecr.Repository(self, "TiktokMetadataRepo",
            repository_name = "tiktok-metadata",
            removal_policy = RemovalPolicy.DESTROY,
            lifecycle_rules = [
                ecr.LifecycleRule(
                    max_image_count = 1,
                    description = "Keep only latest image"
                )
            ]
        )

        self.transcription_repository = ecr.Repository(self, "TiktokTranscriberRepo",
            repository_name = "tiktok-transcriber",
            removal_policy = RemovalPolicy.DESTROY,
            lifecycle_rules = [
                ecr.LifecycleRule(
                    max_image_count = 1,
                    description = "Keep only latest image"
                )
            ]
        )

        self.text_analysis_repository = ecr.Repository(self, "TiktokTextAnalysisRepo",
            repository_name = "tiktok-text-analysis",
            removal_policy = RemovalPolicy.DESTROY,
            lifecycle_rules = [
                ecr.LifecycleRule(
                    max_image_count = 1,
                    description = "Keep only latest image"
                )
            ]
        )

        # Outputs
        CfnOutput(self, "MetadataRepositoryUri",
            value = self.metadata_repository.repository_uri,
            description = "The URI of the metadata ECR repository",
            export_name = "TiktokMetadataRepoUri"
        )

        CfnOutput(self, "TranscriberRepositoryUri",
            value = self.transcription_repository.repository_uri,
            description = "The URI of the transcriber ECR repository",
            export_name = "TiktokTranscriberRepoUri"
        )

        CfnOutput(self, "TextAnalysisRepositoryUri",
            value = self.text_analysis_repository.repository_uri,
            description = "The URI of the text analysis ECR repository",
            export_name = "TiktokTextAnalysisRepoUri"
        )

    @property
    def metadata_repo_uri(self) -> str:
        """Get the URI of the metadata repository"""
        return self.metadata_repository.repository_uri

    @property
    def transcriber_repo_uri(self) -> str:
        """Get the URI of the transcriber repository"""
        return self.transcription_repository.repository_uri

    @property
    def text_analysis_repo_uri(self) -> str:
        """Get the URI of the text analysis repository"""
        return self.text_analysis_repository.repository_uri 