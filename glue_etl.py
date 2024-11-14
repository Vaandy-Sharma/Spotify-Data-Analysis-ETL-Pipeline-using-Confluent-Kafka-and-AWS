import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node ARTISTS
ARTISTS_node1731496597026 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://devandykafka/staging/artists/"], "recurse": True}, transformation_ctx="ARTISTS_node1731496597026")

# Script generated for node TRACKS
TRACKS_node1731496523760 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://devandykafka/staging/tracks/"], "recurse": True}, transformation_ctx="TRACKS_node1731496523760")

# Script generated for node ALBUMS
ALBUMS_node1731496598396 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://devandykafka/staging/albums/"], "recurse": True}, transformation_ctx="ALBUMS_node1731496598396")

# Script generated for node Join
Join_node1731496682165 = Join.apply(frame1=TRACKS_node1731496523760, frame2=ALBUMS_node1731496598396, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Join_node1731496682165")

# Script generated for node Join
Join_node1731497246776 = Join.apply(frame1=ARTISTS_node1731496597026, frame2=Join_node1731496682165, keys1=["id"], keys2=["artist_id"], transformation_ctx="Join_node1731497246776")

# Script generated for node Drop Fields
DropFields_node1731497604004 = DropFields.apply(frame=Join_node1731497246776, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1731497604004")

# Script generated for node Amazon S3
AmazonS3_node1731497777950 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1731497604004, connection_type="s3", format="glueparquet", connection_options={"path": "s3://devandykafka/datelake/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1731497777950")

job.commit()