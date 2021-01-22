import json
import os
import re
from datetime import datetime
from pprint import pformat
from statistics import mean

import boto3
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import TweetTokenizer, sent_tokenize

ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
sagemaker = boto3.client('runtime.sagemaker')
dynamodb = boto3.client('dynamodb')

tokenizer = TweetTokenizer()
stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))


def lambda_handler(event, context):

    data = json.loads(event["body"])
    print(f"User input:\n{pformat(data)}")

    # Get feature values
    # time
    post_datetime = datetime.strptime(
        data["postDate"]+data["postTime"], "%Y-%m-%d%H:%M")
    post_hour = post_datetime.hour
    post_dayofweek = int(post_datetime.strftime("%w")) + 1
    post_month = post_datetime.month
    post_year = post_datetime.year

    # post body
    post_body = data["postBody"]
    post_body_clean = re.sub(
        " +", " ", re.sub("\n", " ", re.sub("<.*?>", "", post_body.replace("&nbsp;", " ")))).strip()
    post_body_clean_nocode = re.sub(
        " +", " ", re.sub("\n", " ", re.sub("<.*?>", "", re.sub(r"(?s)<code>.*?<\/code>", "", post_body.replace("&nbsp;", " "))))).strip()

    post_body_char_count = len(post_body_clean)
    post_body_nocode_char_count = len(post_body_clean_nocode)
    post_body_code_perc = 1-len(post_body_clean_nocode) / \
        len(post_body_clean) if len(post_body_clean) > 0 else 0
    post_body_code_flag = 1 if re.search(
        r"(?s)<code>.*<\/code>", post_body) else 0
    post_body_image_flag = 1 if re.search(
        r"(?s)<img.*>", post_body) else 0
    post_body_link_flag = 1 if re.search(
        r"(?s)<a href>.*<\/a>", post_body) else 0
    post_body_bold_flag = 1 if (re.search(
        r"(?s)<strong>.*<\/strong>", post_body) or re.search(
        r"(?s)<b>.*<\/b>", post_body)) else 0

    post_body_tokens_normalized = [re.sub('[^A-Za-z0-9\']', '', w)
                                   for w in tokenizer.tokenize(post_body_clean_nocode) if re.sub('[^A-Za-z0-9\']', '', w)]
    post_body_pos = pos_tag(post_body_tokens_normalized)
    post_body_stem = [stemmer.stem(
        w) for w in post_body_tokens_normalized if w.lower() not in stop_words]

    post_body_sentence_count = len(sent_tokenize(post_body_clean_nocode))
    post_body_word_count = len(post_body_tokens_normalized)
    post_body_word_distinct_count = len(set(post_body_stem))
    post_body_verb_perc = len([p for p in post_body_pos if p[1].startswith(
        "V")])/post_body_word_count if post_body_word_count > 0 else 0
    post_body_noun_perc = len([p for p in post_body_pos if p[1].startswith(
        "N")])/post_body_word_count if post_body_word_count > 0 else 0
    post_body_pronoun_perc = len([p for p in post_body_pos if p[1].startswith(
        "PR")])/post_body_word_count if post_body_word_count > 0 else 0
    post_body_adjective_perc = len(
        [p for p in post_body_pos if p[1].startswith("J")])/post_body_word_count if post_body_word_count > 0 else 0
    post_body_adverb_perc = len(
        [p for p in post_body_pos if p[1].startswith("RB")])/post_body_word_count if post_body_word_count > 0 else 0

    # post title
    post_title = data["postTitle"]
    post_title_upper_flag = 1 if post_title[0].isupper() else 0
    post_title_question_flag = 1 if post_title.endswith("?") else 0
    post_title_char_count = len(post_title)

    post_title_tokens_normalized = [re.sub('[^A-Za-z0-9\']', '', w)
                                    for w in tokenizer.tokenize(post_title) if re.sub('[^A-Za-z0-9\']', '', w)]
    post_title_pos = pos_tag(post_title_tokens_normalized)
    post_title_stem = [stemmer.stem(
        w) for w in post_title_tokens_normalized if w.lower() not in stop_words]

    post_title_word_count = len(post_title_tokens_normalized)
    post_title_word_distinct_count = len(set(post_title_stem))
    post_title_verb_perc = len([p for p in post_title_pos if p[1].startswith(
        "V")])/post_title_word_count if post_title_word_count > 0 else 0
    post_title_noun_perc = len([p for p in post_title_pos if p[1].startswith(
        "N")])/post_title_word_count if post_title_word_count > 0 else 0
    post_title_pronoun_perc = len([p for p in post_title_pos if p[1].startswith(
        "PR")])/post_title_word_count if post_title_word_count > 0 else 0
    post_title_adjective_perc = len(
        [p for p in post_title_pos if p[1].startswith("J")])/post_title_word_count if post_title_word_count > 0 else 0
    post_title_adverb_perc = len(
        [p for p in post_title_pos if p[1].startswith("RB")])/post_title_word_count if post_title_word_count > 0 else 0

    post_title_in_body_perc = len(set(post_title_stem).intersection(
        set(post_body_stem)))/post_title_word_distinct_count if post_title_word_distinct_count > 0 else 0

    # user
    forum_name = data["forumName"].lower().replace(".", "-")
    user_id = data["userId"]
    # get data from DynamoDB
    user_data = dynamodb.get_item(
        TableName='users',
        Key={
            'id': {
                'S': f'{forum_name}:{user_id}'
            }
        },
    ).get('Item', {})
    user_vars = {
        var: list(value_dict.values())[0] for var, value_dict in user_data.items()
    }
    user_display_name = user_vars.get("user_display_name", "")

    user_age_days = int(user_vars.get("user_age_days", 0))
    user_website_flag = int(user_vars.get("user_website_flag", 0))
    user_location_flag = int(user_vars.get("user_location_flag", 0))
    user_about_me_flag = int(user_vars.get("user_about_me_flag", 0))
    user_badge_1_count = int(user_vars.get("user_badge_1_count", 0))
    user_badge_2_count = int(user_vars.get("user_badge_2_count", 0))
    user_badge_3_count = int(user_vars.get("user_badge_3_count", 0))
    user_post_count = int(user_vars.get("user_post_count", 0))
    user_answer_count = int(user_vars.get("user_answer_count", 0))
    user_first_post_flag = int(user_vars.get("user_first_post_flag", 1))
    user_first_question_flag = int(
        user_vars.get("user_first_question_flag", 1))
    user_answered_questions_count = int(
        user_vars.get("user_answered_questions_count", 0))
    user_accepted_answers_count = int(
        user_vars.get("user_accepted_answers_count", 0))
    user_score = int(user_vars.get("user_score", 0))
    user_question_score = int(user_vars.get("user_question_score", 0))
    user_answer_score = int(user_vars.get("user_answer_score", 0))

    # tags
    post_tags = [t.strip() for t in data["postTags"].split(",")]
    post_tag_count = len(post_tags)

    # get data from DynamoDB
    tag_data = dynamodb.batch_get_item(
        RequestItems={
            'tags': {
                'Keys': [
                    {'id':
                        {
                            'S': f'{forum_name}:{t}'}
                     }
                    for t in post_tags
                ]},
        }
    ).get('Responses').get('tags', [])
    valid_tags = [list(t['id'].values())[0].split(":")[1] for t in tag_data]

    if any(tag_data):
        tag_post_count_avg = mean(
            [int(list(t['tag_post_count'].values())[0]) for t in tag_data])
        tag_post_count_30d_avg = mean(
            [int(list(t['tag_post_count_30d'].values())[0]) for t in tag_data])
        tag_post_count_365d_avg = mean(
            [int(list(t['tag_post_count_365d'].values())[0]) for t in tag_data])
        tag_age_days_avg = mean(
            [int(list(t['tag_age_days'].values())[0]) for t in tag_data])
    else:
        tag_post_count_avg = 0
        tag_post_count_30d_avg = 0
        tag_post_count_365d_avg = 0
        tag_age_days_avg = 0

    # forum
    forum_model_name = forum_name.replace("-", "_")+"_flag"
    android_stackexchange_com_flag = 1 if forum_model_name == "android_stackexchange_com_flag" else 0
    askubuntu_com_flag = 1 if forum_model_name == "askubuntu_com_flag" else 0
    cs_stackexchange_com_flag = 1 if forum_model_name == "cs_stackexchange_com_flag" else 0
    datascience_stackexchange_com_flag = 1 if forum_model_name == "datascience_stackexchange_com_flag" else 0
    dba_stackexchange_com_flag = 1 if forum_model_name == "dba_stackexchange_com_flag" else 0
    devops_stackexchange_com_flag = 1 if forum_model_name == "devops_stackexchange_com_flag" else 0
    gamedev_stackexchange_com_flag = 1 if forum_model_name == "gamedev_stackexchange_com_flag" else 0
    raspberrypi_stackexchange_com_flag = 1 if forum_model_name == "raspberrypi_stackexchange_com_flag" else 0
    softwareengineering_stackexchange_com_flag = 1 if forum_model_name == "softwareengineering_stackexchange_com_flag" else 0
    unix_stackexchange_com_flag = 1 if forum_model_name == "unix_stackexchange_com_flag" else 0

    # Invoke endpoint
    payload = {"schema": {
        "input": [
            {
                "name": "post_hour",
                "type": "int"
            },
            {
                "name": "post_dayofweek",
                "type": "int"
            },
            {
                "name": "post_month",
                "type": "int"
            },
            {
                "name": "post_year",
                "type": "int"
            },
            {
                "name": "post_body_char_count",
                "type": "int"
            },
            {
                "name": "post_body_nocode_char_count",
                "type": "int"
            },
            {
                "name": "post_body_code_perc",
                "type": "double"
            },
            {
                "name": "post_body_code_flag",
                "type": "int"
            },
            {
                "name": "post_body_image_flag",
                "type": "int"
            },
            {
                "name": "post_body_link_flag",
                "type": "int"
            },
            {
                "name": "post_body_bold_flag",
                "type": "int"
            },
            {
                "name": "post_title_upper_flag",
                "type": "int"
            },
            {
                "name": "post_title_question_flag",
                "type": "int"
            },
            {
                "name": "post_title_char_count",
                "type": "int"
            },
            {
                "name": "post_tag_count",
                "type": "int"
            },
            {
                "name": "post_body_sentence_count",
                "type": "int"
            },
            {
                "name": "post_body_word_distinct_count",
                "type": "int"
            },
            {
                "name": "post_body_verb_perc",
                "type": "double"
            },
            {
                "name": "post_body_noun_perc",
                "type": "double"
            },
            {
                "name": "post_body_pronoun_perc",
                "type": "double"
            },
            {
                "name": "post_body_adjective_perc",
                "type": "double"
            },
            {
                "name": "post_body_adverb_perc",
                "type": "double"
            },
            {
                "name": "post_title_word_count",
                "type": "int"
            },
            {
                "name": "post_title_word_distinct_count",
                "type": "int"
            },
            {
                "name": "post_title_verb_perc",
                "type": "double"
            },
            {
                "name": "post_title_noun_perc",
                "type": "double"
            },
            {
                "name": "post_title_pronoun_perc",
                "type": "double"
            },
            {
                "name": "post_title_adjective_perc",
                "type": "double"
            },
            {
                "name": "post_title_adverb_perc",
                "type": "double"
            },
            {
                "name": "post_title_in_body_perc",
                "type": "double"
            },
            {
                "name": "tag_post_count_avg",
                "type": "double"
            },
            {
                "name": "tag_post_count_30d_avg",
                "type": "double"
            },
            {
                "name": "tag_post_count_365d_avg",
                "type": "double"
            },
            {
                "name": "tag_age_days_avg",
                "type": "double"
            },
            {
                "name": "user_age_days",
                "type": "int"
            },
            {
                "name": "user_website_flag",
                "type": "int"
            },
            {
                "name": "user_location_flag",
                "type": "int"
            },
            {
                "name": "user_about_me_flag",
                "type": "int"
            },
            {
                "name": "user_badge_1_count",
                "type": "long"
            },
            {
                "name": "user_badge_2_count",
                "type": "long"
            },
            {
                "name": "user_badge_3_count",
                "type": "long"
            },
            {
                "name": "user_post_count",
                "type": "long"
            },
            {
                "name": "user_answer_count",
                "type": "long"
            },
            {
                "name": "user_first_post_flag",
                "type": "int"
            },
            {
                "name": "user_first_question_flag",
                "type": "int"
            },
            {
                "name": "user_answered_questions_count",
                "type": "long"
            },
            {
                "name": "user_accepted_answers_count",
                "type": "long"
            },
            {
                "name": "user_score",
                "type": "long"
            },
            {
                "name": "user_question_score",
                "type": "long"
            },
            {
                "name": "user_answer_score",
                "type": "long"
            },
            {
                "name": "android_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "askubuntu_com_flag",
                "type": "int"
            },
            {
                "name": "cs_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "datascience_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "dba_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "devops_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "gamedev_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "raspberrypi_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "softwareengineering_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "unix_stackexchange_com_flag",
                "type": "int"
            },
            {
                "name": "y",
                "type": "int"
            }
        ],
        "output":
        {
            "name": "probability",
            "type": "double",
            "struct": "vector"
        }
    },
        "data": [
        post_hour,
        post_dayofweek,
        post_month,
        post_year,
        post_body_char_count,
        post_body_nocode_char_count,
        post_body_code_perc,
        post_body_code_flag,
        post_body_image_flag,
        post_body_link_flag,
        post_body_bold_flag,
        post_title_upper_flag,
        post_title_question_flag,
        post_title_char_count,
        post_tag_count,
        post_body_sentence_count,
        post_body_word_distinct_count,
        post_body_verb_perc,
        post_body_noun_perc,
        post_body_pronoun_perc,
        post_body_adjective_perc,
        post_body_adverb_perc,
        post_title_word_count,
        post_title_word_distinct_count,
        post_title_verb_perc,
        post_title_noun_perc,
        post_title_pronoun_perc,
        post_title_adjective_perc,
        post_title_adverb_perc,
        post_title_in_body_perc,
        tag_post_count_avg,
        tag_post_count_30d_avg,
        tag_post_count_365d_avg,
        tag_age_days_avg,
        user_age_days,
        user_website_flag,
        user_location_flag,
        user_about_me_flag,
        user_badge_1_count,
        user_badge_2_count,
        user_badge_3_count,
        user_post_count,
        user_answer_count,
        user_first_post_flag,
        user_first_question_flag,
        user_answered_questions_count,
        user_accepted_answers_count,
        user_score,
        user_question_score,
        user_answer_score,
        android_stackexchange_com_flag,
        askubuntu_com_flag,
        cs_stackexchange_com_flag,
        datascience_stackexchange_com_flag,
        dba_stackexchange_com_flag,
        devops_stackexchange_com_flag,
        gamedev_stackexchange_com_flag,
        raspberrypi_stackexchange_com_flag,
        softwareengineering_stackexchange_com_flag,
        unix_stackexchange_com_flag,
        0
    ]
    }

    features = (dict(zip([x["name"]
                          for x in payload['schema']['input']], payload['data'])))
    print(f"Calculated features:\n{pformat(features)}")

    response = sagemaker.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                         ContentType='application/json',
                                         Body=json.dumps(payload))

    output = response['Body'].read().decode().split(",")[1]

    print(f"Model response (probability): {output}")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            "probability": output,
            "user_name": user_display_name,
            "tags": {t: True if t in valid_tags else False for t in post_tags}
        }),
        "isBase64Encoded": False
    }
