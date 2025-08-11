import os
from dataall.base import db
from dataall.base.utils.iam_cdk_utils import (
    process_and_split_policy_with_resources_in_statements,
    process_and_split_policy_with_conditions_in_statements,
)
from dataall.core.environment.cdk.pivot_role_stack import PivotRoleStatementSet
from dataall.modules.s3_datasets.db.dataset_repositories import DatasetRepository
from dataall.modules.s3_datasets.db.dataset_models import S3Dataset
from aws_cdk import aws_iam as iam
from sqlalchemy import and_


class DatasetsPivotRole(PivotRoleStatementSet):
    """
    Class including all permissions needed  by the pivot role to work with Datasets based in S3 and Glue databases
    It allows pivot role access to:
    - Athena workgroups for the environment teams
    - Specific Glue databases used by datasets in this environment only
    - Lake Formation resources for specific datasets only
    - Glue ETL for environment resources
    - Imported Datasets' buckets
    - Imported KMS keys alias
    """

    def get_statements(self):
        statements = [
            # For dataset preview
            iam.PolicyStatement(
                sid='AthenaWorkgroupsDataset',
                effect=iam.Effect.ALLOW,
                actions=[
                    'athena:GetQueryExecution',
                    'athena:GetQueryResults',
                    'athena:GetWorkGroup',
                    'athena:StartQueryExecution',
                ],
                resources=[f'arn:aws:athena:*:{self.account}:workgroup/{self.env_resource_prefix}*'],
            ),


            # Glue ETL - needed to start crawler and profiling jobs
            iam.PolicyStatement(
                sid='GlueETL',
                effect=iam.Effect.ALLOW,
                actions=[
                    'glue:StartCrawler',
                    'glue:StartJobRun',
                    'glue:StartTrigger',
                    'glue:UpdateTrigger',
                    'glue:UpdateJob',
                    'glue:UpdateCrawler',
                ],
                resources=[
                    f'arn:aws:glue:*:{self.account}:crawler/{self.env_resource_prefix}*',
                    f'arn:aws:glue:*:{self.account}:job/{self.env_resource_prefix}*',
                    f'arn:aws:glue:*:{self.account}:trigger/{self.env_resource_prefix}*',
                ],
            ),
            iam.PolicyStatement(
                sid='PassRoleGlue',
                actions=[
                    'iam:PassRole',
                ],
                resources=[
                    f'arn:aws:iam::{self.account}:role/{self.env_resource_prefix}*',
                ],
                conditions={
                    'StringEquals': {
                        'iam:PassedToService': [
                            'glue.amazonaws.com',
                        ]
                    }
                },
            ),
        ]
        # Query all datasets (both created and imported) for this environment
        glue_databases = []
        imported_buckets = []
        imported_kms_alias = []

        engine = db.get_engine(envname=os.environ.get('envname', 'local'))
        with engine.scoped_session() as session:
            # Get all active datasets in this environment
            all_datasets = session.query(S3Dataset).filter(
                and_(S3Dataset.environmentUri == self.environmentUri, S3Dataset.deleted.is_(None))
            ).all()
            
            if all_datasets:
                dataset: S3Dataset
                for dataset in all_datasets:
                    # Collect Glue database names for all datasets
                    if dataset.GlueDatabaseName:
                        glue_databases.append(dataset.GlueDatabaseName)
                    
                    # Handle imported datasets (existing S3 bucket logic)
                    if dataset.imported:
                        imported_buckets.append(f'arn:aws:s3:::{dataset.S3BucketName}')
                        if dataset.importedKmsKey:
                            imported_kms_alias.append(f'alias/{dataset.KmsAlias}')

        # Add Glue permissions for specific databases only
        if glue_databases:
            glue_database_resources = [f'arn:aws:glue:*:{self.account}:catalog']
            glue_database_resources.extend([f'arn:aws:glue:*:{self.account}:database/{db}' for db in glue_databases])
            glue_database_resources.extend([f'arn:aws:glue:*:{self.account}:table/{db}/*' for db in glue_databases])
            
            glue_statements = process_and_split_policy_with_resources_in_statements(
                base_sid='GlueCatalogDatasets',
                effect=iam.Effect.ALLOW.value,
                actions=[
                    'glue:BatchCreatePartition',
                    'glue:BatchDeletePartition',
                    'glue:BatchDeleteTable',
                    'glue:CreateDatabase',
                    'glue:CreatePartition',
                    'glue:CreateTable',
                    'glue:DeleteDatabase',
                    'glue:DeletePartition',
                    'glue:DeleteTable',
                    'glue:BatchGetPartition',
                    'glue:GetDatabase',
                    'glue:GetDatabases',
                    'glue:GetTable',
                    'glue:GetTables',
                    'glue:GetPartition',
                    'glue:GetPartitions',
                    'glue:SearchTables',
                    'glue:UpdateDatabase',
                    'glue:UpdatePartition',
                    'glue:UpdateTable',
                    'glue:TagResource',
                    'glue:DeleteResourcePolicy',
                    'glue:PutResourcePolicy',
                ],
                resources=glue_database_resources,
            )
            statements.extend(glue_statements)
            
            # Add Lake Formation permissions for specific databases
            lf_resources = [f'arn:aws:lakeformation:*:{self.account}:catalog']
            lf_resources.extend([f'arn:aws:lakeformation:*:{self.account}:database/{db}' for db in glue_databases])
            lf_resources.extend([f'arn:aws:lakeformation:*:{self.account}:table/{db}/*' for db in glue_databases])
            
            lf_statements = process_and_split_policy_with_resources_in_statements(
                base_sid='LakeFormationDatasets',
                effect=iam.Effect.ALLOW.value,
                actions=[
                    'lakeformation:UpdateResource',
                    'lakeformation:DescribeResource',
                    'lakeformation:AddLFTagsToResource',
                    'lakeformation:RemoveLFTagsFromResource',
                    'lakeformation:GetResourceLFTags',
                    'lakeformation:ListLFTags',
                    'lakeformation:CreateLFTag',
                    'lakeformation:GetLFTag',
                    'lakeformation:UpdateLFTag',
                    'lakeformation:DeleteLFTag',
                    'lakeformation:SearchTablesByLFTags',
                    'lakeformation:SearchDatabasesByLFTags',
                    'lakeformation:ListResources',
                    'lakeformation:ListPermissions',
                    'lakeformation:GrantPermissions',
                    'lakeformation:BatchGrantPermissions',
                    'lakeformation:RevokePermissions',
                    'lakeformation:BatchRevokePermissions',
                    'lakeformation:PutDataLakeSettings',
                    'lakeformation:GetDataLakeSettings',
                    'lakeformation:GetDataAccess',
                    'lakeformation:GetWorkUnits',
                    'lakeformation:StartQueryPlanning',
                    'lakeformation:GetWorkUnitResults',
                    'lakeformation:GetQueryState',
                    'lakeformation:GetQueryStatistics',
                    'lakeformation:GetTableObjects',
                    'lakeformation:UpdateTableObjects',
                    'lakeformation:DeleteObjectsOnCancel',
                ],
                resources=lf_resources,
            )
            statements.extend(lf_statements)

        if imported_buckets:
            dataset_statements = process_and_split_policy_with_resources_in_statements(
                base_sid='ImportedDatasetBuckets',
                effect=iam.Effect.ALLOW.value,
                actions=[
                    's3:List*',
                    's3:GetBucket*',
                    's3:GetLifecycleConfiguration',
                    's3:GetObject',
                    's3:PutBucketPolicy',
                    's3:PutBucketTagging',
                    's3:PutObjectAcl',
                    's3:PutBucketOwnershipControls',
                ],
                resources=imported_buckets,
            )
            statements.extend(dataset_statements)
        if imported_kms_alias:
            kms_statements = process_and_split_policy_with_conditions_in_statements(
                base_sid='KMSImportedDataset',
                effect=iam.Effect.ALLOW.value,
                actions=[
                    'kms:Decrypt',
                    'kms:Encrypt',
                    'kms:GenerateDataKey*',
                    'kms:GetKeyPolicy',
                    'kms:PutKeyPolicy',
                    'kms:ReEncrypt*',
                    'kms:TagResource',
                    'kms:UntagResource',
                ],
                resources=[f'arn:aws:kms:{self.region}:{self.account}:key/*'],
                condition_dict={
                    'key': 'ForAnyValue:StringLike',
                    'resource': 'kms:ResourceAliases',
                    'values': imported_kms_alias,
                },
            )
            statements.extend(kms_statements)

        return statements