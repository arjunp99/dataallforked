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


class DatasetsPivotRole(PivotRoleStatementSet):
    """
    Class including all permissions needed  by the pivot role to work with Datasets based in S3 and Glue databases
    It allows pivot role access to:
    - Athena workgroups for the environment teams
    - All Glue catalog resources (governed by Lake Formation)
    - Lake Formation
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
            # Minimal Glue catalog discovery permissions
            iam.PolicyStatement(
                sid='GlueCatalogDiscovery',
                effect=iam.Effect.ALLOW,
                actions=[
                    'glue:GetDatabases',
                    'glue:GetTables',
                    'glue:SearchTables',
                ],
                resources=[f'arn:aws:glue:*:{self.account}:catalog'],
            ),
            # Database creation permissions for dataall-managed databases
            iam.PolicyStatement(
                sid='GlueDataAllDatabaseCreation',
                effect=iam.Effect.ALLOW,
                actions=[
                    'glue:CreateDatabase',
                    'glue:TagResource',
                ],
                resources=[
                    f'arn:aws:glue:*:{self.account}:catalog',
                    f'arn:aws:glue:*:{self.account}:database/{self.env_resource_prefix}*',
                ],
            ),
            # Global LakeFormation operations (account-level)
            iam.PolicyStatement(
                sid='LakeFormationGlobal',
                effect=iam.Effect.ALLOW,
                actions=[
                    'lakeformation:ListLFTags',
                    'lakeformation:CreateLFTag',
                    'lakeformation:GetLFTag',
                    'lakeformation:UpdateLFTag',
                    'lakeformation:DeleteLFTag',
                    'lakeformation:SearchTablesByLFTags',
                    'lakeformation:SearchDatabasesByLFTags',
                    'lakeformation:ListResources',
                    'lakeformation:ListPermissions',
                    'lakeformation:PutDataLakeSettings',
                    'lakeformation:GetDataLakeSettings',
                ],
                resources=['*'],
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
        # Adding permissions for Imported Dataset S3 Buckets, created bucket permissions are added in core S3 permissions
        # Adding permissions for Imported KMS keys
        # Adding permissions for Imported Glue databases
        imported_buckets = []
        imported_kms_alias = []
        imported_glue_resources = []

        engine = db.get_engine(envname=os.environ.get('envname', 'local'))
        with engine.scoped_session() as session:
            datasets = DatasetRepository.query_environment_imported_datasets(
                session, uri=self.environmentUri, filter=None
            )
            if datasets:
                dataset: S3Dataset
                for dataset in datasets:
                    imported_buckets.append(f'arn:aws:s3:::{dataset.S3BucketName}')
                    if dataset.importedKmsKey:
                        imported_kms_alias.append(f'alias/{dataset.KmsAlias}')
                    # Add Glue database and table ARNs for imported datasets
                    if dataset.importedGlueDatabase:
                        imported_glue_resources.extend([
                            f'arn:aws:glue:*:{self.account}:database/{dataset.GlueDatabaseName}',
                            f'arn:aws:glue:*:{self.account}:table/{dataset.GlueDatabaseName}/*'
                        ])

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
        if imported_glue_resources:
            glue_statements = process_and_split_policy_with_resources_in_statements(
                base_sid='ImportedGlueDatabases',
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
                    'glue:BatchGet*',
                    'glue:Get*',
                    'glue:List*',
                    'glue:SearchTables',
                    'glue:UpdateDatabase',
                    'glue:UpdatePartition',
                    'glue:UpdateTable',
                    'glue:TagResource',
                    'glue:DeleteResourcePolicy',
                    'glue:PutResourcePolicy',
                ],
                resources=imported_glue_resources,
            )
            statements.extend(glue_statements)
        if imported_glue_resources:
            lf_statements = process_and_split_policy_with_resources_in_statements(
                base_sid='ImportedLakeFormationResources',
                effect=iam.Effect.ALLOW.value,
                actions=[
                    'lakeformation:UpdateResource',
                    'lakeformation:DescribeResource',
                    'lakeformation:AddLFTagsToResource',
                    'lakeformation:RemoveLFTagsFromResource',
                    'lakeformation:GetResourceLFTags',
                    'lakeformation:GrantPermissions',
                    'lakeformation:BatchGrantPermissions',
                    'lakeformation:RevokePermissions',
                    'lakeformation:BatchRevokePermissions',
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
                resources=imported_glue_resources,
            )
            statements.extend(lf_statements)

        return statements
