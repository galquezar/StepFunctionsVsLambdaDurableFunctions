import json

def lambda_handler(event, context):
    event['payment'] = True
    return event
