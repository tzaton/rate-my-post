def lambda_handler(event, context):
    output = str(event)
    return {'statusCode': 200,
            'headers': {'Content-Type': 'text/plain',
                        'Access-Control-Allow-Origin': '*'},
            'body': output
            }
