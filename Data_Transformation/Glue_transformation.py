import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Replacing values
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import when

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = df.withColumn(
        "vict_sex",
        when(df.vict_sex == "H", "X")
        .when(df.vict_sex == "-", "X")
        .when(df.vict_sex == "N", "")
        .otherwise(df.vict_sex),
    )

    df = df.withColumn(
        "vict_age",
        when(df.vict_age > 100, "").when(df.vict_age < 0, "").otherwise(df.vict_age),
    )

    df = df.withColumn(
        "weapon_used_id",
        when(df.weapon_used_id == 500, "").otherwise(df.weapon_used_id),
    )

    df = df.withColumn(
        "status",
        when(df.status == "19", "TH")
        .when(df.status == "CC", "TH")
        .when(df.status == "", "TH")
        .otherwise(df.status),
    )

    newcust_df = DynamicFrame.fromDF(df, glueContext, "newcust_df")
    return DynamicFrameCollection({"CustomTransform0": newcust_df}, glueContext)


# Script generated for node SQL Transformation
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from pyspark.sql import SQLContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark_session = glueContext.spark_session
    sqlContext = SQLContext(spark_session.sparkContext, spark_session)
    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = sqlContext.sql(
        """
    SELECT dr_no, DATE(date_rptd), DATE(date_occur), time_occ,area,area_name,rpt_dist_no,part,crm_cd,crm_cd_desc,mocode,vict_age,vict_sex,vict_descent,premise_cd,premise_desc, weapon_used_cd,weapon_desc,status,status_desc,crm_cd1,crm_cd2,location,cross_street,latitude,longitude 
    FROM myDataSource
    """
    )

    newcust_df = DynamicFrame.fromDF(df, glueContext, "newcust_df")
    return DynamicFrameCollection({"CustomTransform0": newcust_df}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1637019918992 = glueContext.create_dynamic_frame.from_catalog(
    database="crime",
    table_name="analysiscrime_dataset",
    transformation_ctx="AWSGlueDataCatalog_node1637019918992",
)

# Script generated for node Apply Mapping
ApplyMapping_node1637020197920 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1637019918992,
    mappings=[
        ("col0", "string", "dr_no", "bigint"),
        ("col1", "string", "date_rptd", "timestamp"),
        ("col2", "string", "date_occur", "timestamp"),
        ("col3", "string", "time_occ", "varchar"),
        ("col4", "string", "area", "bigint"),
        ("col5", "string", "area_name", "string"),
        ("col6", "string", "rpt_dist_no", "bigint"),
        ("col9", "string", "crm_cd_desc", "string"),
        ("col11", "string", "vict_age", "int"),
        ("col12", "string", "vict_sex", "string"),
        ("col13", "string", "vict_descent", "string"),
        ("col14", "string", "premise_id", "int"),
        ("col15", "string", "premise_desc", "string"),
        ("col16", "string", "weapon_used_id", "int"),
        ("col17", "string", "weapon_desc", "string"),
        ("col18", "string", "status", "string"),
        ("col19", "string", "status_desc", "string"),
        ("col20", "string", "crm_id", "int"),
        ("col24", "string", "location", "string"),
        ("col25", "string", "cross_street", "string"),
        ("col26", "string", "latitude", "float"),
        ("col27", "string", "longitude", "float"),
    ],
    transformation_ctx="ApplyMapping_node1637020197920",
)

# Script generated for node Replacing values
Replacingvalues_node1637103918196 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node1637020197920": ApplyMapping_node1637020197920}, glueContext
    ),
)

# Script generated for node SQL Transformation
SQLTransformation_node1638493427825 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node1637020197920": ApplyMapping_node1637020197920}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1637112904139 = SelectFromCollection.apply(
    dfc=Replacingvalues_node1637103918196,
    key=list(Replacingvalues_node1637103918196.keys())[0],
    transformation_ctx="SelectFromCollection_node1637112904139",
)

# Script generated for node snowflakeConnector
snowflakeConnector_node1638122494742 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1637112904139,
    connection_type="custom.jdbc",
    connection_options={
        "tableName": "CRIMETRANSFORMED",
        "dbTable": "CRIMETRANSFORMED",
        "connectionName": "snowflakeConnection",
    },
    transformation_ctx="snowflakeConnector_node1638122494742",
)

job.commit()
