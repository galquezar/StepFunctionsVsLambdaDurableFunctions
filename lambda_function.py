"""
Order processing workflow using AWS Durable Execution SDK.

Orchestrates a multi-step order pipeline: stock validation, total calculation,
payment processing, and shipment confirmation. Steps are replayed idempotently
on retries, and the workflow suspends (via callback) while waiting for external
warehouse confirmation before completing.
"""

import json
import boto3

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import BatchResult, DurableContext, StepContext, durable_step
from aws_durable_execution_sdk_python.execution import durable_execution

dynamodb = boto3.resource("dynamodb")
WAREHOUSE_TABLE = dynamodb.Table("Warehouse")

sqs = boto3.client("sqs")
ORDERS_QUEUE_URL = sqs.get_queue_url(QueueName="OrdersQueue")["QueueUrl"]


def map_items(ctx: DurableContext, item: dict, index: int, items: list[dict]) -> dict:
    """Mapper function passed to context.map — runs get_stock for each item in parallel."""
    return ctx.step(get_stock(item), name=f"GetStock-{index}")


@durable_step
def get_stock(step_context: StepContext, item: dict) -> dict:
    """Fetch current stock units from DynamoDB and attach them to the item.

    Args:
        step_context: Injected by the durable SDK; provides logging and replay state.
        item: Order item dict containing at least an 'id' key.

    Returns:
        The same item dict with a 'units' key added from the Warehouse table.
    """
    step_context.logger.info(f"Fetching item {item['id']} from database")
    response = WAREHOUSE_TABLE.get_item(Key={"id": item["id"]})
    item["units"] = int(response["Item"]["units"])
    return item


@durable_step
def validate_stock(step_context: StepContext, items: list) -> list:
    """Filter out items that have no available stock.

    Returns:
        Subset of items where units > 0.
    """
    step_context.logger.info(f"Removing out of stock items")
    return [item for item in items if item.get("units", 0) > 0]


@durable_step
def calculate_total(step_context: StepContext, items: list) -> float:
    """Sum the price of all available items.

    Returns:
        Total order price as a float; 0.0 if the list is empty.
    """
    step_context.logger.info(f"Calculating order total")
    return sum(item.get("price", 0) for item in items)


def send_approval_request(ctx: DurableContext, callback, request_id: str) -> None:
    """Publish the callback ID to OrdersQueue so the warehouse can resume this workflow.

    The warehouse system must call back with the callback_id to unblock callback.result().

    Args:
        ctx: DurableContext used for structured logging.
        callback: Durable callback object whose callback_id is the resume token.
        request_id: AWS request ID of the current Lambda invocation, used for correlation.
    """
    ctx.logger.info(f"Sending message to warehose to confirm shipment")
    ctx.logger.info(f"CallbackID: {callback.callback_id}")
    sqs.send_message(
        QueueUrl=ORDERS_QUEUE_URL,
        MessageBody=json.dumps({
            "callback_id": callback.callback_id,
            "request_id": request_id,
        }),
    )

@durable_step
def send_email(step_context: StepContext, event: dict) -> dict:
    """Send a shipment confirmation email to the customer.

    Args:
        step_context: Injected by the durable SDK; provides logging and replay state.
        event: Order payload forwarded as-is; expected to contain customer email details.

    Returns:
        The event dict unchanged (placeholder for email service integration).
    """
    step_context.logger.info(f"Sending email to customer")
    return event

@durable_execution
def lambda_handler(event, context: DurableContext) -> dict:
    """Durable order processing pipeline.

    Steps (each is replayed idempotently on retry):
      1. GetStock    — parallel DynamoDB lookup for each item in event['items'].
      2. ValidateStock — removes out-of-stock items.
      3. CalculateTotal — computes event['total'].
      4. ProcessPayment — delegates to PaymentFunction Lambda.
      5. ShipOrder — suspends until the warehouse confirms shipment via SQS callback.
      6. SendEmail - send email to customer to confirm shipment.

    Args:
        event: Order payload. Expected keys: 'items' (list of item dicts with 'id' and 'price').
        context: DurableContext provided by the SDK; wraps the standard Lambda context.

    Returns:
        The enriched event dict including available items, total, and payment result.
    """
    warehouse_items: BatchResult[dict] = context.map(
        event.get("items", []),
        map_items,
        name="map"
    )

    available_items = context.step(
        validate_stock(warehouse_items.get_results()),
        name="ValidateStock"
    )
    event["items"] = available_items

    event["total"] = context.step(
        calculate_total(available_items),
        name="CalculateTotal"
    )

    event = context.invoke(
        "PaymentFunction",
        event,
        name="ProcessPayment"
    )

    # Execution suspends here until the warehouse calls back with the callback_id.
    callback = context.create_callback(name="ShipOrder")
    send_approval_request(context, callback, context.lambda_context.aws_request_id)
    result = callback.result()
    context.logger.info(result)

    result = context.step(
        send_email(event),
        name="SendEmail"
    )

    return event