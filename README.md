This is a workflow example to compare head-to-head Step Functions state machines with Lambda Durable Functions. The workflow is a fictional online order process.

You will need a couple of applications installed in your machine before you can deploy it:
- [aws-cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [sam-cli](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

Then, you can deploy it like that:

```
sam build
sam deploy --guided --capabilities CAPABILITY_NAMED_IAM
```

Once is deployed, you will need to created the following items in the DynamoDB table

| id | name | units |
|----|------|-------|
| 3 | Onions Bag | 0 |
| 1 | Cavendish Banana | 10 |
| 0 | Hass Avocado | 10 |

After that, you are ready to run it.

`command-list` contains the AWS CLI commands to resume the execution of the Step Function state machine and the Lambda Durable Function.
