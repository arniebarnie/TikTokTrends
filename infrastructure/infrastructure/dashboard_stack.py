from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_glue as glue,
    aws_secretsmanager as secretsmanager,
    aws_ssm as ssm,
    aws_lambda as lambda_,
    Duration,
    CfnOutput
)
from constructs import Construct

class DashboardStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Glue Database
        database = glue.CfnDatabase(
            self, 
            "GlueTikTokDB",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="tiktok_analysis",
                description="Database for TikTok video analysis"
            )
        )

        # Create Glue Table
        glue_table = glue.CfnTable(
            self,
            "GlueTikTokTable",
            catalog_id=self.account,
            database_name=database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="tiktok_data",
                description="TikTok video data and analysis results",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="id", type="string"),
                        glue.CfnTable.ColumnProperty(name="title", type="string"),
                        glue.CfnTable.ColumnProperty(name="description", type="string"),
                        glue.CfnTable.ColumnProperty(name="upload_date", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="like_count", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="repost_count", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="comment_count", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="view_count", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="duration", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="transcript", type="string"),
                        glue.CfnTable.ColumnProperty(name="profile_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="processed_at", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="category", type="string"),
                        glue.CfnTable.ColumnProperty(name="summary", type="string"),
                        glue.CfnTable.ColumnProperty(name="keywords", type="array<string>"),
                        glue.CfnTable.ColumnProperty(name="language", type="string")
                    ],
                    location="s3://tiktoktrends/data",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={
                            "serialization.format": "1"
                        }
                    )
                ),
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "parquet.compress": "SNAPPY",
                    "has_encrypted_data": "false"
                }
            )
        )

        # Add dependency
        glue_table.add_dependency(database)

        # Create security group for MySQL
        mysql_security_group = ec2.SecurityGroup(
            self, "MySQLSecurityGroup",
            vpc=vpc,
            description="Security group for MySQL database",
            allow_all_outbound=True
        )

        mysql_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(3306),
            description="Allow MySQL access from within VPC"
        )

        # Add SSH access
        mysql_security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="Allow SSH access from anywhere"
        )

        # Generate MySQL root password and store in Secrets Manager
        db_root_password = secretsmanager.Secret(self, "DBRootPassword",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_characters="\"@/\\ '",
                password_length=16
            )
        )

        # Create EC2 instance role
        ec2_role = iam.Role(self, "EC2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com")
        )
        
        # Allow EC2 to read the secret
        db_root_password.grant_read(ec2_role)

        # Add managed policies for Systems Manager
        ec2_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
        )

        # Add explicit Secrets Manager permissions
        ec2_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            resources=[db_root_password.secret_arn]
        ))

        # Create EC2 instance
        instance = ec2.Instance(self, "MySQLInstance",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
            machine_image=ec2.AmazonLinuxImage(
                generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2
            ),
            security_group=mysql_security_group,
            role=ec2_role,
            key_pair=ec2.KeyPair.from_key_pair_name(
                self, "ImportedKeyPair", 
                key_pair_name="tiktok-analytics-key"
            ),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=10,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        delete_on_termination=True
                    )
                )
            ]
        )

        # Add user data script to install and configure MySQL
        instance.user_data.add_commands(
            "yum update -y",
            
            # Enable and install MariaDB 10.5
            "amazon-linux-extras enable mariadb10.5",
            "yum clean metadata",
            "yum install -y mariadb mariadb-server",
            
            # Configure MySQL to listen on all interfaces
            "echo '[mysqld]' > /etc/my.cnf.d/mysql-bind.cnf",
            "echo 'bind-address = 0.0.0.0' >> /etc/my.cnf.d/mysql-bind.cnf",
            
            "systemctl start mariadb",
            "systemctl enable mariadb",
            
            # Get root password from Secrets Manager and set it
            f"""password=$(aws secretsmanager get-secret-value \
                --secret-id {db_root_password.secret_arn} \
                --query SecretString \
                --output text --region {Stack.of(self).region})""",
            
            'mysqladmin -u root password "$password"',
            
            # Basic security measures and user setup
            'mysql -u root -p"$password" -e "DELETE FROM mysql.user WHERE User=\'\';"',
            'mysql -u root -p"$password" -e "DROP DATABASE IF EXISTS test;"',
            'mysql -u root -p"$password" -e "DELETE FROM mysql.db WHERE Db=\'test\' OR Db=\'test\\_%\';"',
            
            # Create root user that can connect from any host
            f'mysql -u root -p"$password" -e "CREATE USER \'root\'@\'%\' IDENTIFIED BY \'$password\';"',
            f'mysql -u root -p"$password" -e "GRANT ALL PRIVILEGES ON *.* TO \'root\'@\'%\' WITH GRANT OPTION;"',
            'mysql -u root -p"$password" -e "FLUSH PRIVILEGES;"',
            
            # Create application database
            'mysql -u root -p"$password" -e "CREATE DATABASE tiktok_analytics;"',
            
            # Restart MySQL to apply configuration changes
            "systemctl restart mariadb"
        )

        # Store instance info in SSM Parameter Store for Lambda to use
        ssm.StringParameter(self, "DBEndpoint",
            parameter_name="/tiktok-analytics/db-endpoint",
            string_value=instance.instance_private_ip
        )

        # # Create Lambda role
        # lambda_role = iam.Role(self, "LambdaRole",
        #     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        # )

        # # Allow Lambda to access the database password
        # db_root_password.grant_read(lambda_role)

        # # Allow Lambda to access VPC resources
        # lambda_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole")
        # )

        # # Add SSM Parameter Store permissions
        # lambda_role.add_to_policy(iam.PolicyStatement(
        #     effect=iam.Effect.ALLOW,
        #     actions=[
        #         "ssm:GetParameter",
        #         "ssm:GetParameters"
        #     ],
        #     resources=[
        #         f"arn:aws:ssm:{self.region}:{self.account}:parameter/tiktok-analytics/*"
        #     ]
        # ))

        # # Create query Lambda
        # query_lambda = lambda_.Function(self, "QueryLambda",
        #     runtime=lambda_.Runtime.PYTHON_3_9,
        #     handler="main.handler",
        #     code=lambda_.Code.from_asset("lambda/query/lambda_function.zip"),
        #     timeout=Duration.minutes(5),
        #     memory_size=1024,
        #     environment={
        #         "DB_SECRET_ARN": db_root_password.secret_arn
        #     },
        #     vpc=vpc,
        #     vpc_subnets=ec2.SubnetSelection(
        #         subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        #     ),
        #     security_groups=[mysql_security_group],
        #     role=lambda_role
        # )

        # Add outputs
        CfnOutput(self, "GlueDatabaseName",
            value=database.ref,
            description="The name of the Glue database"
        )

        CfnOutput(self, "GlueTableName",
            value="tiktok_data",
            description="The name of the Glue table"
        )

        CfnOutput(self, "InstanceId",
            value=instance.instance_id,
            description="EC2 instance ID"
        )
        
        CfnOutput(self, "DBSecretArn",
            value=db_root_password.secret_arn,
            description="Database root password secret ARN"
        )

        # CfnOutput(self, "QueryLambdaName",
        #     value=query_lambda.function_name,
        #     description="Query Lambda function name"
        # ) 