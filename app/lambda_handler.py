def lambda_handler(event, context):

    output = 0.5
    return {'statusCode': 200,
            'headers': {'Content-Type': 'text/plain',
                        'Access-Control-Allow-Origin': '*'},
            'body': output
            }
