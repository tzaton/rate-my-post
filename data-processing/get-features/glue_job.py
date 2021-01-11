import sys
import time

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'database_name'])
database_name = args["database_name"]


def save_table(df, table_name):
    print(f"Writing to DynamoDB table: {table_name}")
    glue_context.write_dynamic_frame_from_options(
        frame=DynamicFrame.fromDF(df, glue_context, table_name),
        connection_type="dynamodb",
        connection_options={
            "dynamodb.output.tableName": table_name
        }
    )
    print(f"Table {table_name} updated")


spark.sql(f"""
use {database_name}
""")

current_date = spark.sql(
    "select max(creation_date) from posts").collect()[0][0]
current_timestamp = time.mktime(current_date.timetuple())

print(f"Data valid as of: {current_date}")

# user status
user_status = spark.sql(f"""
select
    u.dataset_name,
    u.id as user_id,
    u.display_name as user_display_name,
    datediff(from_unixtime({current_timestamp}), u.creation_date) as user_age_days,

    case when u.website_url is not null then 1 else 0 end as user_website_flag,
    case when u.location is not null then 1 else 0 end as user_location_flag,
    case when u.about_me is not null then 1 else 0 end as user_about_me_flag,

    count(b.name) as n_badges,
    count(case when b.class = 1 then b.name end) as n_badges_class_1,
    count(case when b.class = 2 then b.name end) as n_badges_class_2,
    count(case when b.class = 3 then b.name end) as n_badges_class_3
from users u
left join badges b
    on u.id = b.user_id
    and u.dataset_name = b.dataset_name
group by
    u.dataset_name,
    u.id,
    u.display_name,
    u.creation_date,
    u.website_url,
    u.location,
    u.about_me
""")

# user history
user_history = spark.sql(f"""
select
    p.dataset_name,
    p.owner_user_id as user_id,

    count(p.id) as n_user_posts,
    count(case when p.post_type_id = 1 then p.id end) as n_user_questions,
    count(case when p.post_type_id = 2 then p.id end) as n_user_answers,
    case when count(p.id) = 0 then 1 else 0 end as user_first_post,
    case when count(case when p.post_type_id = 1 then p.id end) = 0 then 1 else 0 end as user_first_question,

    count(case when p.answer_count > 0 then p.id end) as n_user_answered_questions,
    count(case when p.accepted_answer_id is not null then p.id end) as n_user_accepted_answers,

    coalesce(sum(p.score), 0) as user_score,
    coalesce(sum(case when p.post_type_id = 1 then p.score end), 0) as user_question_score,
    coalesce(sum(case when p.post_type_id = 2 then p.score end), 0) as user_answer_score
from posts p
where p.owner_user_id is not null
group by
    p.dataset_name,
    p.owner_user_id
""")

users = user_status.join(user_history,
                         ["dataset_name", "user_id"],
                         "left")\
    .selectExpr(
    "concat(dataset_name, ':', user_id) as id",
    "user_display_name",
    "user_age_days",
    "user_website_flag",
    "user_location_flag",
    "user_about_me_flag",
    "n_badges as user_badge_count",
    "n_badges_class_1 as user_badge_1_count",
    "n_badges_class_2 as user_badge_2_count",
    "n_badges_class_3 as user_badge_3_count",
    "coalesce(n_user_posts, 0) as user_post_count",
    "coalesce(n_user_questions, 0) as user_question_count",
    "coalesce(n_user_answers, 0) as user_answer_count",
    "coalesce(user_first_post, 1) as user_first_post_flag",
    "coalesce(user_first_question, 1) as user_first_question_flag",
    "coalesce(n_user_answered_questions, 0) as user_answered_questions_count",
    "coalesce(n_user_accepted_answers, 0) as user_accepted_answers_count",
    "coalesce(user_score, 0) as user_score",
    "coalesce(user_question_score, 0) as user_question_score",
    "coalesce(user_answer_score, 0) as user_answer_score"
)

users.show(5, vertical=True, truncate=False)
save_table(users, "users")

# tags
tags = spark.sql(f"""
select
    p.dataset_name,
    p.tag,
    datediff(from_unixtime({current_timestamp}), min(p.creation_date)) as tag_age_days,
    count(*) as n_tag_posts,
    count(case when datediff(from_unixtime({current_timestamp}), p.creation_date) < 30 then p.id end) as n_tag_posts_30d,
    count(case when datediff(from_unixtime({current_timestamp}), p.creation_date) < 365 then p.id end) as n_tag_posts_365d
from (select p.id, p.dataset_name, p.creation_date, explode(p.tags) as tag from posts p where p.post_type_id = 1) p
group by
    p.dataset_name,
    p.tag
""")\
    .selectExpr(
    "concat(dataset_name, ':', tag) as id",
    "tag_age_days",
    "n_tag_posts as tag_post_count",
    "n_tag_posts_30d as tag_post_count_30d",
    "n_tag_posts_365d as tag_post_count_365d"
)

tags.show(5, vertical=True, truncate=False)
save_table(tags, "tags")

print("Ending execution")
