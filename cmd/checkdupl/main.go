package main

import (
	"crypto/sha1"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"hash/fnv"
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

	filt := expression.Name("userId").Equal(expression.Value("deuxio5f44d23e4e971"))

	// Or we could get by ratings and pull out those with the right year later
	//    filt := expression.Name("info.rating").GreaterThan(expression.Value(min_rating))

	// Get back the title, year, and rating
	proj := expression.NamesList(
		expression.Name("userId"),
		expression.Name("requestId"),
		expression.Name("receivedResponse"))

	expr, err := expression.NewBuilder().
		WithFilter(filt).
		WithProjection(proj).
		Build()

	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Make the DynamoDB Query API call
	count := 0
	duplicated := make(map[string]interface{})
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

			duplicated[HashSHA1(item.ReceivedResponse)] = struct{}{}
		}
		lastKeyEvaluated = result.LastEvaluatedKey

		if len(lastKeyEvaluated) == 0 {
			break
		} else {
			fmt.Printf("count is: %d, lastKeyEvaluated is: %v", count, lastKeyEvaluated)
		}
	}

	fmt.Printf("Total: %d, Duplicated: %v", count, len(duplicated))
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func HashSHA1(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)

	return fmt.Sprintf("%x\n", bs)
}