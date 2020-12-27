def lambda_handler(event, context):
    output = event["body"]
    return {'statusCode': 200,
            'headers': {'Content-Type': 'text/plain',
                        'Access-Control-Allow-Origin': '*'},
            'body': output
            }
