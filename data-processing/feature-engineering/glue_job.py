import sys

import pyspark.sql.functions as f
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from sparknlp.annotator import *
from sparknlp.base import *

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'input_db',
                                     'output_db',
                                     'model_bucket'])
input_db = args["input_db"]
output_db = args["output_db"]
model_bucket = args["model_bucket"]


def save_table(df, table_name):
    print(f"Saving table: {table_name}")
    spark.sql(f"drop table if exists {output_db}.{table_name}")
    df\
        .write\
        .mode("overwrite")\
        .format("parquet")\
        .saveAsTable(f"{output_db}.{table_name}")
    print(f"Table: {table_name} saved")


spark.sql(f"""
use {input_db}
""")

# question data
questions = spark.sql(f"""
select
    dataset_name,
    post_id,
    post_datetime,
    post_hour,
    post_dayofweek,
    post_month,
    post_year,
    body,
    body_clean,
    body_clean_nocode,

    length(body_clean) as body_n_characters,
    length(body_clean_nocode) as body_nocode_n_characters,

    1-length(body_clean_nocode)/length(body_clean) as body_code_perc,
    case when body like "%<code>%</code>%" then 1 else 0 end as body_code_flag,
    case when body like "%<img%>%" then 1 else 0 end as body_image_flag,
    case when body like "%<a href%</a>%" then 1 else 0 end as body_link_flag,
    case when body like "%<strong>%</strong>%" or body like "%<b>%</b>%" then 1 else 0 end as body_bold_flag,

    title,
    case when title rlike "^[A-Z]" then 1 else 0 end as title_upper_flag,
    case when substr(title, -1, 1) = "?" then 1 else 0 end as title_question_flag,
    length(title) as title_n_characters,

    tags,
    n_tags,

    post_closed_flag,
    post_score,
    post_score_1d,
    post_score_30d
from (
select
    p.dataset_name,
    p.id as post_id,

    p.creation_date as post_datetime,
    hour(p.creation_date) as post_hour,
    dayofweek(p.creation_date) as post_dayofweek,
    month(p.creation_date) as post_month,
    year(p.creation_date) as post_year,

    p.body,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(p.body, "&nbsp;", " "), "<.*?>", ""), "\n", " "), " +", " ")) as body_clean,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(p.body, "&nbsp;", " "), "(?s)<code>.*?<\/code>", ""), "<.*?>", ""), "\n", " "), " +", " ")) as body_clean_nocode,
    p.title,
    p.tags,
    size(p.tags) as n_tags,
    case when p.closed_date is not null then 1 else 0 end as post_closed_flag,
    p.score as post_score,
    count(case when v.vote_type_id = 2 and datediff(v.creation_date, p.creation_date) < 1 then 1 end) -
    count(case when v.vote_type_id = 3 and datediff(v.creation_date, p.creation_date) < 1 then 1 end) as post_score_1d,
    count(case when v.vote_type_id = 2 and datediff(v.creation_date, p.creation_date) < 30 then 1 end) -
    count(case when v.vote_type_id = 3 and datediff(v.creation_date, p.creation_date) < 30 then 1 end) as post_score_30d

from posts p
left join votes v
    on v.post_id = p.id
    and p.dataset_name = v.dataset_name
    and v.vote_type_id in (2, 3, 5)
where p.post_type_id = 1
group by
    p.dataset_name,
    p.id,
    p.owner_user_id,
    p.creation_date,
    p.creation_year,
    p.body,
    p.title,
    p.tags,
    p.closed_date,
    p.score
    )
""")
save_table(questions, "questions")


# user status
user_status = spark.sql(f"""
select distinct
    p.dataset_name,
    p.id as post_id,
    p.owner_user_id as user_id,
    u.display_name as user_display_name,

    datediff(p.creation_date, u.creation_date) as user_age_days,

    case when u.website_url is not null then 1 else 0 end as user_website_flag,
    case when u.location is not null then 1 else 0 end as user_location_flag,
    case when u.about_me is not null then 1 else 0 end as user_about_me_flag,

    count(b.name) as n_badges,
    count(case when b.class = 1 then b.name end) as n_badges_class_1,
    count(case when b.class = 2 then b.name end) as n_badges_class_2,
    count(case when b.class = 3 then b.name end) as n_badges_class_3
from posts p
join users u
    on p.owner_user_id = u.id
    and p.dataset_name = u.dataset_name
left join badges b
    on u.id = b.user_id
    and u.dataset_name = b.dataset_name
    and b.date <= p.creation_date
where p.post_type_id = 1
    and p.owner_user_id is not null
group by
    p.dataset_name,
    p.id,
    p.owner_user_id,
    u.display_name,
    p.creation_date,
    u.creation_date,
    u.website_url,
    u.location,
    u.about_me
""")
save_table(user_status, "user_status")


# user history
user_history = spark.sql(f"""
select
    p.dataset_name,
    p.id as post_id,
    p.owner_user_id as user_id,

    count(p2.id) as n_user_posts,
    count(case when p2.post_type_id = 1 then p2.id end) as n_user_questions,
    count(case when p2.post_type_id = 2 then p2.id end) as n_user_answers,
    case when count(p2.id) = 0 then 1 else 0 end as user_first_post,
    case when count(case when p2.post_type_id = 1 then p2.id end) = 0 then 1 else 0 end as user_first_question,

    count(case when p2.answer_count > 0 then p2.id end) as n_user_answered_questions,
    count(case when p2.accepted_answer_id is not null then p2.id end) as n_user_accepted_answers,

    coalesce(sum(p2.score), 0) as user_score,
    coalesce(sum(case when p2.post_type_id = 1 then p2.score end), 0) as user_question_score,
    coalesce(sum(case when p2.post_type_id = 2 then p2.score end), 0) as user_answer_score
from posts p
left join posts p2
    on p.owner_user_id = p2.owner_user_id
    and p.dataset_name = p2.dataset_name
    and p2.creation_date < p.creation_date
where p.post_type_id = 1
group by
    p.dataset_name,
    p.id,
    p.owner_user_id
""")
save_table(user_history, "user_history")


# answer data
answers = spark.sql(f"""
select
    dataset_name,
    post_id,
    user_id,
    count(answer_id) as n_answers,
    count(case when datediff(answer_datetime, post_datetime) < 1 then answer_id end) as n_answers_1d,
    count(case when datediff(answer_datetime, post_datetime) < 30 then answer_id end) as n_answers_30d,
    max(case when answer_id is not null then 1 else 0 end) as answered_flag,
    max(case when datediff(answer_datetime, post_datetime) < 1 then 1 else 0 end) as answered_1d_flag,
    max(case when datediff(answer_datetime, post_datetime) < 7 then 1 else 0 end) as answered_7d_flag,
    max(case when datediff(answer_datetime, post_datetime) < 14 then 1 else 0 end) as answered_14d_flag,
    max(case when datediff(answer_datetime, post_datetime) < 30 then 1 else 0 end) as answered_30d_flag,
    max(answer_score) as answer_max_score,
    max(answer_score_1d) as answer_max_score_1d,
    max(answer_score_30d) as answer_max_score_30d,
    max(answer_accepted_flag) as answer_accepted_flag,
    max(answer_accepted_1d_flag) as answer_accepted_1d_flag,
    max(answer_accepted_7d_flag) as answer_accepted_7d_flag,
    max(answer_accepted_14d_flag) as answer_accepted_14d_flag,
    max(answer_accepted_30d_flag) as answer_accepted_30d_flag
    from (
select
    p.dataset_name,
    p.id as post_id,
    p.owner_user_id as user_id,
    p.creation_date as post_datetime,

    a.id as answer_id,
    a.creation_date as answer_datetime,
    max(case when va.vote_type_id = 1 then 1 else 0 end) as answer_accepted_flag,
    max(case when va.vote_type_id = 1 and datediff(va.creation_date, p.creation_date) < 1 then 1 else 0 end) as answer_accepted_1d_flag,
    max(case when va.vote_type_id = 1 and datediff(va.creation_date, p.creation_date) < 7 then 1 else 0 end) as answer_accepted_7d_flag,
        max(case when va.vote_type_id = 1 and datediff(va.creation_date, p.creation_date) < 14 then 1 else 0 end) as answer_accepted_14d_flag,
    max(case when va.vote_type_id = 1 and datediff(va.creation_date, p.creation_date) < 30 then 1 else 0 end) as answer_accepted_30d_flag,

    coalesce(a.score, 0) as answer_score,
    count(case when va.vote_type_id = 2 and datediff(va.creation_date, p.creation_date) < 1 then 1 end) -
    count(case when va.vote_type_id = 3 and datediff(va.creation_date, p.creation_date) < 1 then 1 end) as answer_score_1d,
    count(case when va.vote_type_id = 2 and datediff(va.creation_date, p.creation_date) < 30 then 1 end) -
    count(case when va.vote_type_id = 3 and datediff(va.creation_date, p.creation_date) < 30 then 1 end) as answer_score_30d
from posts p
left join posts a
    on p.id = a.parent_id
    and p.dataset_name = a.dataset_name
    and a.post_type_id = 2
left join votes va
    on va.post_id = a.id
    and va.dataset_name = a.dataset_name
    and va.vote_type_id in (1, 2, 3)
where p.post_type_id = 1
and ((a.owner_user_id != p.owner_user_id) or a.owner_user_id is null)
group by
    p.dataset_name,
    p.id,
    p.owner_user_id,
    p.creation_date,
    a.id,
    a.creation_date,
    a.score
    ) group by dataset_name, post_id, user_id
""")
save_table(answers, "answers")


# tag data
tags = spark.sql("""
select
    dataset_name,
    post_id,
    max(n_tag_posts) as n_tag_posts_max,
    max(n_tag_posts_30d) as n_tag_posts_max_30d,
    max(n_tag_posts_365d) as n_tag_posts_max_365d,
    max(tag_age_days) as tag_age_days_max,
    avg(n_tag_posts) as n_tag_posts_avg,
    avg(n_tag_posts_30d) as n_tag_posts_avg_30d,
    avg(n_tag_posts_365d) as n_tag_posts_avg_365d,
    avg(tag_age_days) as tag_age_days_avg
    from (
        select
            p.dataset_name,
            p.id as post_id,
            p.tag,
            datediff(p.creation_date, t.tag_start_date) as tag_age_days,
            count(p2.tag) as n_tag_posts,
            count(case when datediff(p.creation_date, p2.creation_date) < 30 then p2.id end) as n_tag_posts_30d,
            count(case when datediff(p.creation_date, p2.creation_date) < 365 then p2.id end) as n_tag_posts_365d
        from (select p.id, p.dataset_name, p.creation_date, explode(p.tags) as tag from posts p where p.post_type_id = 1) p
        left join (select p.id, p.dataset_name, p.creation_date, explode(p.tags) as tag from posts p where p.post_type_id = 1) p2
            on p.tag = p2.tag
            and p.dataset_name = p2.dataset_name
            and p2.creation_date < p.creation_date
        join (select
                p.dataset_name,
                p.tag,
                min(p.creation_date) as tag_start_date
            from (select p.id, p.dataset_name, p.creation_date, explode(p.tags) as tag from posts p where p.post_type_id = 1) p
            group by
                p.dataset_name,
                p.tag) t
            on p.tag = t.tag
            and p.dataset_name = t.dataset_name
        group by
            p.dataset_name,
            p.id,
            p.tag,
            p.creation_date,
            t.tag_start_date
    )
    group by
        dataset_name, post_id
""")
save_table(tags, "tags")

# question data - NLP features
questions_nlp_base = spark.sql("""
select
    p.dataset_name,
    p.id as post_id,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(p.body, "&nbsp;", " "), "(?s)<code>.*?<\/code>", ""), "<.*?>", ""), "\n", " "), " +", " ")) as body_clean_nocode,
    p.title
from posts p
where p.post_type_id = 1
""")

# Spark NLP Annotators
documentAssemblerBody = DocumentAssembler() \
    .setInputCol("body_clean_nocode") \
    .setOutputCol("document")

documentAssemblerTitle = DocumentAssembler() \
    .setInputCol("title") \
    .setOutputCol("document")

sentenceDetector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

regexTokenizer = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token")

normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normalized")\
    .setCleanupPatterns(["[^A-Za-z0-9\']"])

pos = PerceptronModel.load(f"s3://{model_bucket}/pos_anc_en_2.0.2_2.4_1556659930154/")\
    .setInputCols("document", "normalized")\
    .setOutputCol("pos")

stopwords_cleaner = StopWordsCleaner()\
    .setInputCols("normalized")\
    .setOutputCol("removed_stopwords")\
    .setCaseSensitive(False)\

stemmer = Stemmer() \
    .setInputCols(["removed_stopwords"]) \
    .setOutputCol("stem")

# Body processing pipeline
pipelineBody = Pipeline() \
    .setStages([
        documentAssemblerBody,
        sentenceDetector,
        regexTokenizer,
        normalizer,
        pos,
        stopwords_cleaner,
        stemmer
    ])

# Title processing pipeline
pipelineTitle = Pipeline() \
    .setStages([
        documentAssemblerTitle,
        sentenceDetector,
        regexTokenizer,
        normalizer,
        pos,
        stopwords_cleaner,
        stemmer
    ])

nlp1 = pipelineBody.fit(questions_nlp_base).transform(questions_nlp_base)\
    .select(*questions_nlp_base.columns,
            f.size("sentence.result").alias("body_n_sentences"),
            f.size("normalized.result").alias("body_n_words"),
            f.size(f.array_distinct("stem.result")).alias(
                "body_n_distinct_words"),
            f.size(f.expr("filter(pos.result, x -> x like 'V%')")
                   ).alias("body_n_verbs"),
            f.size(f.expr("filter(pos.result, x -> x like 'N%')")
                   ).alias("body_n_nouns"),
            f.size(f.expr("filter(pos.result, x -> x like 'PR%')")
                   ).alias("body_n_pronouns"),
            f.size(f.expr("filter(pos.result, x -> x like 'J%')")
                   ).alias("body_n_adjectives"),
            f.size(f.expr("filter(pos.result, x -> x like 'RB%')")
                   ).alias("body_n_adverbs"),
            f.array_distinct(f.col("stem.result")).alias("body_words")
            )

questions_nlp = pipelineTitle.fit(nlp1).transform(nlp1)\
    .select(*nlp1.columns,
            f.size("normalized.result").alias("title_n_words"),
            f.size(f.array_distinct("stem.result")).alias(
                "title_n_distinct_words"),
            f.size(f.expr("filter(pos.result, x -> x like 'V%')")
                   ).alias("title_n_verbs"),
            f.size(f.expr("filter(pos.result, x -> x like 'N%')")
                   ).alias("title_n_nouns"),
            f.size(f.expr("filter(pos.result, x -> x like 'PR%')")
                   ).alias("title_n_pronouns"),
            f.size(f.expr("filter(pos.result, x -> x like 'J%')")
                   ).alias("title_n_adjectives"),
            f.size(f.expr("filter(pos.result, x -> x like 'RB%')")
                   ).alias("title_n_adverbs"),
            f.array_distinct(f.col("stem.result")).alias("title_words")
            )\
    .withColumn("title_in_body_perc",
                f.size(f.array_intersect(f.col("title_words"), f.col("body_words")))/f.col("title_n_distinct_words"))\
    .selectExpr("dataset_name",
                "post_id",
                "body_clean_nocode",
                "title",
                "body_n_sentences",
                "body_n_words",
                "body_n_distinct_words",
                "body_n_verbs",
                "body_n_nouns",
                "body_n_pronouns",
                "body_n_adjectives",
                "body_n_adverbs",
                "title_n_words",
                "title_n_distinct_words",
                "title_n_verbs",
                "title_n_nouns",
                "title_n_pronouns",
                "title_n_adjectives",
                "title_n_adverbs",
                "title_in_body_perc")\
    .repartition(80)
save_table(questions_nlp, "questions_nlp")

# Final model dataset
spark.sql(f"""
use {output_db}
""")

model_data = spark.sql("""
select
    -- features
    q.dataset_name,
    q.post_id,
    q.post_datetime,
    q.post_hour,
    q.post_dayofweek,
    q.post_month,
    q.post_year,
    q.body as post_body,
    q.body_n_characters as post_body_char_count,
    q.body_nocode_n_characters as post_body_nocode_char_count,
    q.body_code_perc as post_body_code_perc,
    q.body_code_flag as post_body_code_flag,
    q.body_image_flag as post_body_image_flag,
    q.body_link_flag as post_body_link_flag,
    q.body_bold_flag as post_body_bold_flag,
    q.title as post_title,
    q.title_upper_flag as post_title_upper_flag,
    q.title_question_flag as post_title_question_flag,
    q.title_n_characters as post_title_char_count,
    q.tags as post_tags,
    q.n_tags as post_tag_count,
    coalesce(q2.body_n_sentences, 0) as post_body_sentence_count,
    coalesce(q2.body_n_words, 0) as post_body_word_count,
    coalesce(q2.body_n_distinct_words, 0) as post_body_word_distinct_count,
    coalesce(q2.body_n_verbs/q2.body_n_words, 0) as post_body_verb_perc,
    coalesce(q2.body_n_nouns/q2.body_n_words, 0) as post_body_noun_perc,
    coalesce(q2.body_n_pronouns/q2.body_n_words, 0) as post_body_pronoun_perc,
    coalesce(q2.body_n_adjectives/q2.body_n_words, 0) as post_body_adjective_perc,
    coalesce(q2.body_n_adverbs/q2.body_n_words, 0) as post_body_adverb_perc,
    coalesce(q2.title_n_words, 0) as post_title_word_count,
    coalesce(q2.title_n_distinct_words, 0) as post_title_word_distinct_count,
    coalesce(q2.title_n_verbs/q2.title_n_words, 0) as post_title_verb_perc,
    coalesce(q2.title_n_nouns/q2.title_n_words, 0) as post_title_noun_perc,
    coalesce(q2.title_n_pronouns/q2.title_n_words, 0) as post_title_pronoun_perc,
    coalesce(q2.title_n_adjectives/q2.title_n_words, 0) as post_title_adjective_perc,
    coalesce(q2.title_n_adverbs/q2.title_n_words, 0) as post_title_adverb_perc,
    coalesce(q2.title_in_body_perc, 0) as post_title_in_body_perc,
    t.n_tag_posts_max as tag_post_count_max,
    t.n_tag_posts_max_30d as tag_post_count_30d_max,
    t.n_tag_posts_max_365d as tag_post_count_365d_max,
    t.tag_age_days_max,
    t.n_tag_posts_avg as tag_post_count_avg,
    t.n_tag_posts_avg_30d as tag_post_count_30d_avg,
    t.n_tag_posts_avg_365d as tag_post_count_365d_avg,
    t.tag_age_days_avg,
    us.user_id,
    us.user_age_days,
    us.user_website_flag,
    us.user_location_flag,
    us.user_about_me_flag,
    us.n_badges as user_badge_count,
    us.n_badges_class_1 as user_badge_1_count,
    us.n_badges_class_2 as user_badge_2_count,
    us.n_badges_class_3 as user_badge_3_count,
    uh.n_user_posts as user_post_count,
    uh.n_user_questions as user_question_count,
    uh.n_user_answers as user_answer_count,
    uh.user_first_post as user_first_post_flag,
    uh.user_first_question as user_first_question_flag,
    uh.n_user_answered_questions as user_answered_questions_count,
    uh.n_user_accepted_answers as user_accepted_answers_count,
    uh.user_score,
    uh.user_question_score,
    uh.user_answer_score,
    -- target
    q.post_closed_flag,
    q.post_score,
    q.post_score_1d,
    q.post_score_30d,
    a.n_answers as answer_count,
    a.n_answers_1d as answer_1d_count,
    a.n_answers_30d as answer_30d_count,
    a.answered_flag as answer_flag,
    a.answered_1d_flag as answer_1d_flag,
    a.answered_7d_flag as answer_7d_flag,
    a.answered_14d_flag as answer_14d_flag,
    a.answered_30d_flag as answer_30d_flag,
    a.answer_max_score,
    a.answer_max_score_1d,
    a.answer_max_score_30d,
    a.answer_accepted_flag,
    a.answer_accepted_1d_flag,
    a.answer_accepted_7d_flag,
    a.answer_accepted_14d_flag,
    a.answer_accepted_30d_flag
from questions q
join user_status us
    on q.dataset_name = us.dataset_name
    and q.post_id = us.post_id
join user_history uh
    on q.dataset_name = uh.dataset_name
    and q.post_id = uh.post_id
join tags t
    on q.dataset_name = t.dataset_name
    and q.post_id = t.post_id
join answers a
    on q.dataset_name = a.dataset_name
    and q.post_id = a.post_id
join questions_nlp q2
    on q.dataset_name = q2.dataset_name
    and q.post_id = q2.post_id
where
    q.body_n_characters > 0
    and q.title_n_characters > 0
""")

# Forum - binary columns
forums = sorted(model_data.select(
    "dataset_name").distinct().rdd.map(lambda x: x[0]).collect())

model_data = model_data\
    .select(*(model_data.columns) + [f.when(f.col("dataset_name") == x, 1).otherwise(0).alias(f"{x.replace('-', '_')}_flag") for x in forums])

save_table(model_data, "model_data")

print("Ending execution")
