import os  # Import the os module to interact with environment variables
import boto3  # Import the boto3 library to interact with AWS services

def handler(event, context):
    # Extract the raw path from the event data
    path = event["rawPath"]
    # If the path is not the root path, return a 404 Not Found response
    if path != "/":
        return {"statusCode": 404, "body": "Not found."}

    # Initialize a DynamoDB resource
    dynamodb = boto3.resource("dynamodb")
    # Get a reference to the DynamoDB table using the table name from environment variables
    table = dynamodb.Table(os.environ.get("TABLE_NAME"))

    # Try to read the "visit_count" item from the DynamoDB table
    response = table.get_item(Key={"key": "visit_count"})
    # Check if the item exists in the response
    if "Item" in response:
        # If the item exists, get the current visit count
        visit_count = response["Item"]["value"]
    else:
        # If the item does not exist, initialize the visit count to 0
        visit_count = 0

    # Increment the visit count by 1
    new_visit_count = visit_count + 1
    # Write the new visit count back to the DynamoDB table
    table.put_item(Item={"key": "visit_count", "value": new_visit_count})

    # Get the version from the environment variables, defaulting to "0.0" if not set
    version = os.environ.get("VERSION", "0.0")
    # Create the response body with a message, version, and visit count
    response_body = {
        "message": "Hello Oscar 👋",
        "version": version,
        "visit_count": new_visit_count,
    }
    # Return a 200 OK response with the response body
    return {"statusCode": 200, "body": response_body}
