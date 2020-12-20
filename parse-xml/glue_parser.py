import logging
import sys

import pyspark.sql.functions as f
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'bucket_name',
                                     'input_dir',
                                     'output_dir'])

bucket_name = args['bucket_name']
input_dir = args['input_dir']
output_dir = args['output_dir']


def save_table(df, table_name, partition_keys=None):
    logger.info("Saving table: %s", table_name)
    output_path = f"s3://{bucket_name}/{output_dir}/{table_name}"

    df = df\
        .withColumn('dataset_name',
                    f.split(f.split(f.input_file_name(), '/').getItem(f.size(f.split(f.input_file_name(), '/'))-1), '\.').getItem(0))

    if partition_keys is not None:
        df\
            .repartition(*partition_keys)\
            .write\
            .mode("overwrite")\
            .format("parquet")\
            .partitionBy(*partition_keys)\
            .save(output_path)
    else:
        df\
            .coalesce(1)\
            .write\
            .mode("overwrite")\
            .format("parquet")\
            .save(output_path)
    logger.info("Table: %s saved at: %s", table_name, output_path)


# badges
badges = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/badges')\
    .withColumnRenamed('_Class', 'class')\
    .withColumnRenamed('_Date', 'date')\
    .withColumnRenamed('_Id', 'id')\
    .withColumnRenamed('_Name', 'name')\
    .withColumnRenamed('_TagBased', 'tag_based')\
    .withColumnRenamed('_UserId', 'user_id')\
    .select('id',
            'user_id',
            'name',
            'date',
            'class',
            'tag_based')\
    .withColumn("date",
                f.to_timestamp(f.col("date")))\
    .withColumn("year", f.year("date"))

save_table(badges, "badges", ["year"])


# comments
comments = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/comments')\
    .withColumnRenamed("_ContentLicense", "content_license")\
    .withColumnRenamed("_CreationDate", "creation_date")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_PostId", "post_id")\
    .withColumnRenamed("_Score", "score")\
    .withColumnRenamed("_Text", "text")\
    .withColumnRenamed("_UserDisplayName", "user_display_name")\
    .withColumnRenamed("_UserId", "user_id")\
    .select("id",
            "post_id",
            "score",
            "text",
            "creation_date",
            "user_id",
            "content_license")\
    .withColumn("creation_date",
                f.to_timestamp(f.col("creation_date")))\
    .withColumn("creation_year", f.year("creation_date"))

save_table(comments, "comments", ["creation_year"])


# post history
post_history = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/post_history')\
    .withColumnRenamed("_Comment", "comment")\
    .withColumnRenamed("_ContentLicense", "content_license")\
    .withColumnRenamed("_CreationDate", "creation_date")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_PostHistoryTypeId", "post_history_type_id")\
    .withColumnRenamed("_PostId", "post_id")\
    .withColumnRenamed("_RevisionGUID", "revision_guid")\
    .withColumnRenamed("_Text", "text")\
    .withColumnRenamed("_UserDisplayName", "user_display_name")\
    .withColumnRenamed("_UserId", "user_id")\
    .select("id",
            "post_history_type_id",
            "post_id",
            "revision_guid",
            "creation_date",
            "user_id",
            "text",
            "content_license")\
    .withColumn("creation_date",
                f.to_timestamp(f.col("creation_date")))\
    .withColumn("creation_year", f.year("creation_date"))

save_table(post_history, "post_history", ["creation_year"])


# post links
post_links = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/post_links')\
    .withColumnRenamed("_CreationDate", "creation_date")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_LinkTypeId", "link_type_id")\
    .withColumnRenamed("_PostId", "post_id")\
    .withColumnRenamed("_RelatedPostId", "related_post_id")\
    .select("id",
            "creation_date",
            "post_id",
            "related_post_id",
            "link_type_id")\
    .withColumn("creation_date",
                f.to_timestamp(f.col("creation_date")))\
    .withColumn("creation_year", f.year("creation_date"))

save_table(post_links, "post_links", ["creation_year"])


# posts
posts = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/posts')\
    .withColumnRenamed("_AcceptedAnswerId", "accepted_answer_id")\
    .withColumnRenamed("_AnswerCount", "answer_count")\
    .withColumnRenamed("_Body", "body")\
    .withColumnRenamed("_ClosedDate", "closed_date")\
    .withColumnRenamed("_CommentCount", "comment_count")\
    .withColumnRenamed("_CommunityOwnedDate", "community_owned_date")\
    .withColumnRenamed("_ContentLicense", "content_license")\
    .withColumnRenamed("_CreationDate", "creation_date")\
    .withColumnRenamed("_FavoriteCount", "favorite_count")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_LastActivityDate", "last_activity_date")\
    .withColumnRenamed("_LastEditDate", "last_edit_date")\
    .withColumnRenamed("_LastEditorDisplayName", "last_editor_display_name")\
    .withColumnRenamed("_LastEditorUserId", "last_editor_user_id")\
    .withColumnRenamed("_OwnerDisplayName", "owner_display_name")\
    .withColumnRenamed("_OwnerUserId", "owner_user_id")\
    .withColumnRenamed("_ParentId", "parent_id")\
    .withColumnRenamed("_PostTypeId", "post_type_id")\
    .withColumnRenamed("_Score", "score")\
    .withColumnRenamed("_Tags", "tags")\
    .withColumnRenamed("_Title", "title")\
    .withColumnRenamed("_ViewCount", "view_count")\
    .select("id",
            "post_type_id",
            "parent_id",
            "accepted_answer_id",
            "creation_date",
            "score",
            "view_count",
            "body",
            "owner_user_id",
            "owner_display_name",
            "last_editor_user_id",
            "last_editor_display_name",
            "last_edit_date",
            "last_activity_date",
            "title",
            "tags",
            "answer_count",
            "comment_count",
            "favorite_count",
            "community_owned_date",
            "closed_date",
            "content_license")\
    .withColumn("creation_date",
                f.to_timestamp(f.col("creation_date")))\
    .withColumn("last_edit_date",
                f.to_timestamp(f.col("last_edit_date")))\
    .withColumn("last_activity_date",
                f.to_timestamp(f.col("last_activity_date")))\
    .withColumn("community_owned_date",
                f.to_timestamp(f.col("community_owned_date")))\
    .withColumn("closed_date",
                f.to_timestamp(f.col("closed_date")))\
    .withColumn("tags",
                f.split(f.rtrim(f.regexp_replace(f.regexp_replace(f.col("tags"), "<", ""), ">", " ")), " "))\
    .withColumn("creation_year", f.year("creation_date"))

save_table(posts, "posts", ["creation_year"])


# tags
tags = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/tags')\
    .withColumnRenamed("_Count", "count")\
    .withColumnRenamed("_ExcerptPostId", "excerpt_post_id")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_TagName", "tag_name")\
    .withColumnRenamed("_WikiPostId", "wiki_post_id")\
    .select("id",
            "tag_name",
            "count",
            "excerpt_post_id",
            "wiki_post_id")

save_table(tags, "tags")


# users
users = spark.read.format('xml')\
    .options(rowTag='row')\
    .load(f's3://{bucket_name}/{input_dir}/users')\
    .withColumnRenamed("_AboutMe", "about_me")\
    .withColumnRenamed("_AccountId", "account_id")\
    .withColumnRenamed("_CreationDate", "creation_date")\
    .withColumnRenamed("_DisplayName", "display_name")\
    .withColumnRenamed("_DownVotes", "down_votes")\
    .withColumnRenamed("_Id", "id")\
    .withColumnRenamed("_LastAccessDate", "last_access_date")\
    .withColumnRenamed("_Location", "location")\
    .withColumnRenamed("_ProfileImageUrl", "profile_image_url")\
    .withColumnRenamed("_Reputation", "reputation")\
    .withColumnRenamed("_UpVotes", "up_votes")\
    .withColumnRenamed("_Views", "views")\
    .withColumnRenamed("_WebsiteUrl", "website_url")\
    .select("id",
            "reputation",
            "creation_date",
            "display_name",
            "last_access_date",
            "website_url",
            "location",
            "about_me",
            "views",
            "up_votes",
            "down_votes",
            "profile_image_url",
            "account_id")\
    .withColumn("creation_date",
                f.to_timestamp(f.col("creation_date")))\
    .withColumn("last_access_date",
                f.to_timestamp(f.col("last_access_date")))\
    .withColumn("creation_year", f.year("creation_date"))

save_table(users, "users", ["creation_year"])

logger.info("Ending execution")
