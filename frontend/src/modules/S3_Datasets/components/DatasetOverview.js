import { Box, Grid, Chip } from '@mui/material';
import PropTypes from 'prop-types';
import { ObjectBrief, ObjectMetadata } from 'design';
import { DatasetConsoleAccess } from './DatasetConsoleAccess';
import { DatasetGovernance } from 'modules/DatasetsBase/components/DatasetGovernance';
import { isFeatureEnabled } from 'utils';

export const DatasetOverview = (props) => {
  const { dataset, isAdmin, ...other } = props;
  const showDatasetType = isFeatureEnabled('s3_datasets', 'show_dataset_type');

  return (
    <Grid container spacing={2} {...other}>
      <Grid item lg={7} xl={9} xs={12}>
        <Box sx={{ mb: 3 }}>
          <ObjectBrief
            title="Details"
            uri={dataset.datasetUri || '-'}
            name={
              <>
                {dataset.label || '-'}
                {showDatasetType && (
                  <Chip
                    size="small"
                    label={
                      dataset.imported
                        ? 'Imported S3-Glue Dataset'
                        : 'Created S3-Glue Dataset'
                    }
                    color={dataset.imported ? 'primary' : 'secondary'}
                    sx={{ ml: 1 }}
                  />
                )}
              </>
            }
            description={dataset.description || 'No description provided'}
          />
        </Box>
        <Box sx={{ mb: 3 }}>
          <DatasetGovernance dataset={dataset} />
        </Box>
      </Grid>
      <Grid item lg={5} xl={3} xs={12}>
        <ObjectMetadata
          environment={dataset.environment}
          region={dataset.region}
          organization={dataset.environment?.organization}
          owner={dataset.owner}
          created={dataset.created}
          status={dataset.stack?.status}
          objectType="dataset"
        />
        <Box sx={{ mt: 2 }}>
          {isAdmin && dataset.restricted && (
            <DatasetConsoleAccess dataset={dataset} />
          )}
        </Box>
      </Grid>
    </Grid>
  );
};

DatasetOverview.propTypes = {
  dataset: PropTypes.object.isRequired,
  isAdmin: PropTypes.bool.isRequired
};
