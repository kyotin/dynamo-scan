package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"os"
)

// Create struct to hold info about new item
type Item struct {
	RequestID    string
	UserId       string
	ReceivedResponse string
}

func main() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	// Get back the title, year, and rating
	proj := expression.NamesList(
		expression.Name("userId"),
		expression.Name("requestId"),
		expression.Name("receivedResponse"))

	expr, err := expression.NewBuilder().
		WithProjection(proj).
		Build()

	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Make the DynamoDB Query API call
	count := 0
	userRequests := make(map[string]int64)
	var lastKeyEvaluated map[string]*dynamodb.AttributeValue
	for {
		params := &dynamodb.ScanInput{
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
			TableName:                 aws.String("ApilayerRequest-prod"),
			ExclusiveStartKey:         lastKeyEvaluated,
		}

		result, err := svc.Scan(params)
		if err != nil {
			fmt.Println("Query API call failed:")
			fmt.Println((err.Error()))
			os.Exit(1)
		}

		fmt.Println(*result.Count)
		fmt.Println(len(result.Items))
		for _, i := range result.Items {
			count++
			item := Item{}

			err = dynamodbattribute.UnmarshalMap(i, &item)
			if err != nil {
				fmt.Println("Got error unmarshalling:")
				fmt.Println(err.Error())
				os.Exit(1)
			}

			userRequests[item.UserId] = userRequests[item.UserId] + 1
		}
		lastKeyEvaluated = result.LastEvaluatedKey

		if len(lastKeyEvaluated) == 0 {
			break
		} else {
			fmt.Printf("count is: %d, lastKeyEvaluated is: %v", count, lastKeyEvaluated)
		}
	}

	fmt.Println(userRequests)
}