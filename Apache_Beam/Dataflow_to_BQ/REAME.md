# Writing to BigQuery with Apache beam pipeline

## Description:

This is quite simple pipeline to demonstrate the basic workflow.
It simply cleans the input csv records and sinks them into BQ table.

### Steps to reproduce;

1. This pipeline can be run on the cloud shell, so there is no need for setting environmental variable.

2. In the cloud shell install beam with `pip install apache-beam[gcp]`

3. Create 2 GCS buckets: first for the input csv file and second for temporary files (required by Dataflow)

4. Run the following command: `python script.py --input gs://bucket-name/file_name.csv --output project_id:dataset-name.table_name --temp_location gs://bucket-name`

No need to specify the runner as GCS will automatically utilize Dataflow. Also you won't find the explicit job execution in the Dataflow UI, but that's fine I guess.

