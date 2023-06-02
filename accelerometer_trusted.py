import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Script generated for node Cutomer Curated Zone
CutomerCuratedZone_node1684079837328 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://customerrecords2023/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CutomerCuratedZone_node1684079837328",
)

# Script generated for node Customer privacy filter
Customerprivacyfilter_node1684080571868 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CutomerCuratedZone_node1684079837328,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customerprivacyfilter_node1684080571868",
)

# Script generated for node Drop Fields
DropFields_node1684081032440 = DropFields.apply(
    frame=Customerprivacyfilter_node1684080571868,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1684081032440",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.getSink(
    path="s3://customerrecords2023/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node3",
)
AccelerometerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node3.setFormat("glueparquet")
AccelerometerTrusted_node3.writeFrame(DropFields_node1684081032440)
job.commit()
