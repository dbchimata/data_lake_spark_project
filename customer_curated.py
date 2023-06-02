import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://customerrecords2023/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Cutomer Trusted Zone
CutomerTrustedZone_node1684079837328 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://customerrecords2023/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CutomerTrustedZone_node1684079837328",
)

# Script generated for node Customer privacy filter
Customerprivacyfilter_node1684080571868 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CutomerTrustedZone_node1684079837328,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customerprivacyfilter_node1684080571868",
)

# Script generated for node Drop Fields
DropFields_node1684081032440 = DropFields.apply(
    frame=Customerprivacyfilter_node1684080571868,
    paths=["user", "x", "y", "z"],
    transformation_ctx="DropFields_node1684081032440",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685644251867 = DynamicFrame.fromDF(
    DropFields_node1684081032440.toDF().dropDuplicates(["email", "serialNumber"]),
    glueContext,
    "DropDuplicates_node1685644251867",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685650042106 = DynamicFrame.fromDF(
    DropDuplicates_node1685644251867.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1685650042106",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.getSink(
    path="s3://customerrecords2023/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node3",
)
CustomerCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node3.setFormat("glueparquet")
CustomerCurated_node3.writeFrame(DropDuplicates_node1685650042106)
job.commit()