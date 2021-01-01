import random


def lambda_handler(event, context):

    output = random.random()
    return {'statusCode': 200,
            'headers': {'Content-Type': 'text/plain',
                        'Access-Control-Allow-Origin': '*'},
            'body': output
            }
