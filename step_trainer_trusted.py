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

# Script generated for node customer_curated
customer_curated_node1684101649671 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://customerrecords2023/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1684101649671",
)

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://customerrecords2023/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1684105869049 = ApplyMapping.apply(
    frame=Step_trainer_landing_node1,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1684105869049",
)

# Script generated for node Join
Join_node1684101732920 = Join.apply(
    frame1=customer_curated_node1684101649671,
    frame2=RenamedkeysforJoin_node1684105869049,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node1684101732920",
)

# Script generated for node Drop Fields
DropFields_node1684105500458 = DropFields.apply(
    frame=Join_node1684101732920,
    paths=[
        "phone",
        "lastUpdateDate",
        "email",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay",
        "timeStamp",
        "`(right) serialNumber`",
    ],
    transformation_ctx="DropFields_node1684105500458",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.getSink(
    path="s3://customerrecords2023/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node3",
)
step_trainer_trusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_landing_to_trusted"
)
step_trainer_trusted_node3.setFormat("glueparquet")
step_trainer_trusted_node3.writeFrame(DropFields_node1684105500458)
job.commit()
