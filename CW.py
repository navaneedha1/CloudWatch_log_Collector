# Purpose: Get CloudWatch CPUUtilization and mem_used_percent EC2 metric data, number of Amazon WorkSpaces, AWS Direct Connect(DX) in Connected state, Amazon DynamoDB capacity utilization and finally write to S3 in CSV format. Intent
#          is to use this script to extend the functionality so usage metrics could be included.
#          In the context of the tool, this script would be made into a Lambda in the architecture.
#
# Execution Requirements:
# 1. Underlying Python/AWSCLI environment configured with  an IAM user in the organization's root account. In other
#    words, the script will be run with the organization's root account credentials. This is only because the script was
#    tested/validated that way.
# 2. All child accounts in the organization must have the same role configured to assume. For example, the role
#    'OrganizationTREXAccessRole' was added to the child accounts in the organization used for testing, and that role
#    was assumed in each account to retrieve the metric data. That role had all of the needed permission for
#    CloudWatch and EC2.
# 3. The CloudWatch Agent must be installed on all of the EC2 instances where the metric mem_used_percent is wanted, and
#    the Agent must be configured consistently (e.g. with the same metric namespace) across all instances.
# 3. The target S3 bucket should have a bucket policy that only allows the IAM user in the Org root account running this
#    script to write to it.

import io

import boto3
import logging
import pandas as pd
from datetime import datetime, timedelta
from boto.s3.key import Key
import awswrangler as wr

# GetMetricData Information
# You can use the GetMetricData API to retrieve as many as 500 different metrics
# in a single request,with a total of as many as 100,800 data points
metricPeriod = 300
pageSize = 500
endTime = datetime.now()
startTime = datetime.now() - timedelta(hours=1)
bucketName = "Bucket_Name"
regionList = [] # e.g ["us-west-2"]


def processMetricResults(cw_client, metrics,filename,namespace):
    metricDataQueries = [];

    metricNum = 0;

    for metric in metrics:
        metricNum = metricNum + 1

        metricDataQueries.append(
            {
                'Id': "m" + str(metricNum),
                'MetricStat': {
                    'Metric': {
                        'Namespace': metric["Namespace"],
                        'MetricName': metric["MetricName"],
                        'Dimensions': metric["Dimensions"],
                    },
                    'Period': metricPeriod,
                    'Stat': 'Average'
                   
                },
            }
        );


    #print(metricDataQueries)
    # Get the GMD paginator
    gmdPaginator = cw_client.get_paginator('get_metric_data')

    # Get the GMD iterator
    metricDataIterator = gmdPaginator.paginate(
        MetricDataQueries=metricDataQueries,
        StartTime=startTime,
        EndTime=endTime,
        ScanBy="TimestampDescending",
        PaginationConfig={
            'PageSize': pageSize
        },
    )


    appended_data = []

    for mdrPage in metricDataIterator:

        for mdr in mdrPage["MetricDataResults"]:

            metricId = mdr["Id"]
            label = mdr["Label"]
            timeStamps = mdr["Timestamps"]
            values = mdr["Values"]
            for i, ts in enumerate(timeStamps):
                appended_data.append(accountId + "," + region + "," + str(timeStamps[i]) + "," + str(values[i]) + "," + label.replace(
                    ' ', ','))
    print(appended_data)
    df = pd.read_csv(io.StringIO('\n'.join(appended_data)))
    path = f"s3://{bucketName}/{namespace}/{filename}.csv"
    wr.s3.to_csv(df, path, index=False)

def processMetrics(cw_client, metrics, filename,namespace):
    metricList = []
    for metric in metrics:
        metricList.append(metric)
        if (len(metricList) >= 500):
            processMetricResults(cw_client, metricList,namespace)
            metricList = []

    if (len(metricList)>0):
        processMetricResults(cw_client, metricList, filename,namespace)

def listMetrics(cw_client, dimensions, metrics_list, namespace, filename):
    # List Metric API (get only active metrics using RecentlyActive property)
    # Can be used optimized the number of calls, no need to retrieve "empty" metrics
    # If you are not clear on what metrics you should retrieve you can use this to list ALL the metrics for a namespace
    paginator = cw_client.get_paginator('list_metrics')

    metrics = [];

    for response in paginator.paginate(Dimensions=dimensions,
                                MetricName=metrics_list,
                                Namespace=namespace):
       metrics = metrics + response['Metrics'];

    processMetrics(cw_client, metrics, filename,namespace)


def collectMetrics(cw_client, region):

     # CPUUtilization
    dimensions = [{'Name': 'Instance Name', 'Name': 'InstanceId'}]
    metrics = 'CPUUtilization'
    namespace = 'AWS/EC2'
    filename = 'EC2_CPUUtilization'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    # EC2_NetworkIn
    dimensions = [{'Name': 'Instance Name', 'Name': 'InstanceId'}]
    metrics = 'NetworkIn'
    namespace = 'AWS/EC2'
    filename = 'EC2_NetworkIn'
    listMetrics(cw_client, dimensions,metrics, namespace, filename)

    # EC2_NetworkOut
    dimensions = [{'Name': 'Instance Name', 'Name': 'InstanceId'}]
    metrics = 'NetworkOut'
    namespace = 'AWS/EC2'
    filename = 'EC2_NetworkOut'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    # All Metrics in ContainerInsights Namespace
    namespace = 'ContainerInsights'
    filename = 'ContainerInsights'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    #  Metrics in DirectConnect Namespace
    dimensions = [{'Name': 'ConnectionId'}]
    metrics = 'ConnectionState'
    namespace = 'AWS/DX'
    filename = 'DX_ConnectionState'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    #  Metrics in s3 AllRequests Namespace
    dimensions = [{'Name': 'BucketName', 'Name': 'StorageType'}]
    metrics = 'AllRequests'
    namespace = 'AWS/S3'
    filename = 'S3_BucketSizeBytes'
    listMetrics(cw_client, dimensions,metrics, namespace, filename)

    # Metrics in DynamoDB AccountProvisionedReadCapacityUtilization Namespace
    dimensions = [{'Name':'TableName'}]
    metrics='ProvisionedReadCapacityUnits'
    namespace = 'AWS/DynamoDB'
    filename = 'ProvisionedReadCapacityUnits'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    #  Metrics in DynamoDB AccountProvisionedWriteCapacityUtilization Namespace
    metrics = 'AccountProvisionedWriteCapacityUtilization'
    namespace = 'AWS/DynamoDB'
    filename = 'AccountProvisionedWriteCapacityUtilization'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)
    #
    # #  Metrics in DynamoDB ConsumedReadCapacityUnits Namespace
    metrics = 'ConsumedReadCapacityUnits'
    namespace = 'AWS/DynamoDB'
    filename = 'ConsumedReadCapacityUnits'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    # #  Metrics in DynamoDB ConsumedWriteCapacityUnits Namespace

    metrics = 'ConsumedWriteCapacityUnits'
    namespace = 'AWS/DynamoDB'
    filename = 'ConsumedWriteCapacityUnits'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

    # #  Metrics for the number of WorkSpaces that have a user connected
    dimensions = [{'Name':'DirectoryId'}]
    metrics = 'UserConnected'
    namespace = 'AWS/WorkSpaces'
    filename = 'UserConnected'
    listMetrics(cw_client, dimensions, metrics, namespace, filename)

# Initialize AWS Organizations boto3 client
orgClient = boto3.client('organizations')

# Build list of account IDs in the Org
 accountList = []
 accountDict = orgClient.list_accounts()

 for account in accountDict['Accounts']:
    accountList.append(account['Id'])

# For each account
for accountId in accountList:

    # Initialize STS client
    sts_client = boto3.client('sts')

    # Assume appropriate role. The appropriate role can be the administrator role that is configured when a new
    # account is created in an AWS Org (role automatically added to account) or when an account joins an Org by invite
    # (role must be manually added to account), though it is most likely not a security best practice to use that role
    # because it does not follow the principle of least privilege. The point is to make sure whatever role used is the
    # same role across all accounts in the Org and only has the permissions it needs to accomplish its task. In this
    # case, the role is named 'OrganizationTREXAccessRole'.
     assumed_role_object = sts_client.assume_role(
        RoleArn="arn:aws:iam::" + accountId + ":role/OrganizationAccountAccessRole",
       RoleSessionName="Session-" + accountId,
       DurationSeconds=3600  # 3600s = 1h
     )

    # Store credentials of the assumed role
    credentials = assumed_role_object['Credentials']

    print(accountId)  # for debugging purposes

    # Initiate CloudWatch client in the same region as the EC2 instances
    cw_client = boto3.client(
        'cloudwatch',
         aws_access_key_id=credentials['AccessKeyId'],
         aws_secret_access_key=credentials['SecretAccessKey'],
         aws_session_token=credentials['SessionToken']
    )
    response = cw_client.list_metrics()


    # For each region in the region list
    for region in regionList:
        cw_client.region_name = region
        collectMetrics(cw_client, region)
