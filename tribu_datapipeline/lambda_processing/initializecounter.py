import boto3

def initialize_counter():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('RouteIDCounter')
    
    # Inicializa el contador con el valor 100000
    table.put_item(
        Item={
            'IDType': 'RouteID',
            'CounterValue': 100000
        }
    )
    print("Counter initialized to 100000.")

if __name__ == "__main__":
    initialize_counter()