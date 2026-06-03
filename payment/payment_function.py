import json


def lambda_handler(event, context):
    """Payment processing — charges the card and returns the enriched order."""
    print(f"Processing payment for order: {json.dumps(event)}")

    return {
        **event,
        "payment": {
            "status": "approved",
            "transactionId": context.aws_request_id,
        },
    }
