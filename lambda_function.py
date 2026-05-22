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
    return ctx.step(get_stock(item), name=f"GetStock-{index}")

@durable_step
def get_stock(step_context: StepContext, item: dict) -> dict:
    step_context.logger.info(f"Fetching item {item["id"]} from database")
    response = WAREHOUSE_TABLE.get_item(Key={"id": item["id"]})
    item["units"] = int(response["Item"]["units"])
    return item

@durable_step
def validate_stock(step_context: StepContext, items: list) -> list:
    return [item for item in items if item.get("units", 0) > 0]

@durable_step
def calculate_total(step_context: StepContext, items: list) -> float:
    return sum(item.get("price", 0) for item in items)

def send_approval_request(callback, request_id):
    print(f"CallbackID: {callback.callback_id}")
    sqs.send_message(
        QueueUrl=ORDERS_QUEUE_URL,
        MessageBody=json.dumps({
            "callback_id": callback.callback_id,
            "request_id": request_id,
        }),
    )

@durable_execution
def lambda_handler(event, context: DurableContext) -> dict:
    # GetStock
    warehouse_items: BatchResult[dict] = context.map(
        event.get("items", []),
        map_items,
        name="map"
    )

    # ValidateStock
    available_items = context.step(
        validate_stock(warehouse_items.get_results()),
        name="ValidateStock" 
    )
    event["items"] = available_items

    # CalculateTotal
    event["total"] = context.step(
        calculate_total(available_items),
        name="CalculateTotal" 
    )

    # ProcessPayment
    payment = context.invoke(
        "PaymentFunction",
        event,
        name="ProcessPayment"
    )

    # ShipOrder
    # It stops the execution until the warehouse confirms the order has been sent
    callback = context.create_callback(name="ShipOrder")

    # Send callback.callback_id to the external system that will resume this function.
    send_approval_request(callback, context.lambda_context.aws_request_id)

    # Execution suspends here until the external system calls back.
    result = callback.result()
    print(result)

    return event