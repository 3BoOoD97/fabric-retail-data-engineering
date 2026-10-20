import os
import json
import uuid
import random
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

connection_string = os.environ["FABRIC_EVENTHUB_CONNECTION_STRING"]

products = [
    {"ProductID": "PROD11", "ProductName": "Gaming Keyboard", "CategoryName": "Accessories", "UnitPrice": 49.90},
    {"ProductID": "PROD17", "ProductName": "Wireless Mouse", "CategoryName": "Accessories", "UnitPrice": 29.90},
    {"ProductID": "PROD21", "ProductName": "Milk", "CategoryName": "Dairy & Eggs", "UnitPrice": 2.50},
    {"ProductID": "PROD22", "ProductName": "Eggs", "CategoryName": "Dairy & Eggs", "UnitPrice": 4.90},
    {"ProductID": "PROD31", "ProductName": "Bread", "CategoryName": "Bakery", "UnitPrice": 3.20},
    {"ProductID": "PROD41", "ProductName": "Orange Juice", "CategoryName": "Beverages", "UnitPrice": 3.80},
]

# One execution = one customer order
order_id = f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(100, 999)}"
customer_id = f"CUST{random.randint(5000, 5999)}"


selected_products = random.sample(
    products,
    k=random.randint(1, min(4, len(products)))
)

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string
)

regions = [
    "Marmara",
    "Central Anatolia",
    "Aegean",
    "Mediterranean",
    "Black Sea"
]

region = random.choice(regions)


with producer:
    batch = producer.create_batch()

    for product in selected_products:
        quantity = random.randint(1, 4)
        discount_rate = random.choice([0.0, 0.05, 0.10, 0.15])

        net_amount = round(
            product["UnitPrice"] * quantity * (1 - discount_rate),
            2
        )

        event = {
            "EventID": str(uuid.uuid4()),
            "OrderLineID": str(uuid.uuid4()),
            "OrderID": order_id,
            "EventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "CustomerID": customer_id,
            "ProductID": product["ProductID"],
            "ProductName": product["ProductName"],
            "CategoryName": product["CategoryName"],
            "Quantity": quantity,
            "UnitPrice": product["UnitPrice"],
            "DiscountRate": discount_rate,
            "NetAmount": net_amount,
            "Region": region
        }

        batch.add(EventData(json.dumps(event)))

        print(json.dumps(event, indent=2))

    producer.send_batch(batch)

print(f"\nOrder {order_id} sent successfully")