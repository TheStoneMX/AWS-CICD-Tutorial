// Import necessary CDK libraries and constructs
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as dotenv from "dotenv";

// Define a new CDK stack class
export class AwsCicdTutorialStack extends cdk.Stack {
  // Constructor for the stack class
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    // Call the parent constructor with scope, id, and props
    super(scope, id, props);

    // Load environment variables from a .env file
    dotenv.config();

    // Create a new DynamoDB table to store some data
    const table = new dynamodb.Table(this, "VisitorTimeTable", {
      // Define the partition key for the table
      partitionKey: {
        name: "key",
        type: dynamodb.AttributeType.STRING,
      },
      // Set the billing mode to pay per request
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    });

    // Create a new Lambda function
    const lambdaFunction = new lambda.Function(this, "LambdaFunction", {
      // Specify the runtime environment for the Lambda function
      runtime: lambda.Runtime.PYTHON_3_9,
      // Specify the code location for the Lambda function
      code: lambda.Code.fromAsset("lambda"),
      // Define the handler for the Lambda function
      handler: "main.handler",
      // Set environment variables for the Lambda function
      environment: {
        VERSION: process.env.VERSION || "0.0", // Version from .env file or default to "0.0"
        TABLE_NAME: table.tableName, // DynamoDB table name
      },
    });

    // Grant the Lambda function read and write permissions to the DynamoDB table
    table.grantReadWriteData(lambdaFunction);

    // Create a function URL for the Lambda function
    const functionUrl = lambdaFunction.addFunctionUrl({
      // Set the authorization type to none
      authType: lambda.FunctionUrlAuthType.NONE,
      // Configure CORS settings
      cors: {
        allowedOrigins: ["*"], // Allow all origins
        allowedMethods: [lambda.HttpMethod.ALL], // Allow all HTTP methods
        allowedHeaders: ["*"], // Allow all headers
      },
    });

    // Output the function URL as a CloudFormation stack output
    new cdk.CfnOutput(this, "Url", {
      value: functionUrl.url,
    });
  }
}
