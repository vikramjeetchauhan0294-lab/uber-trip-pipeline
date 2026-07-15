import random
import time
from datetime import datetime

LOCATIONS = [
    "Connaught Place",
    "Karol Bagh",
    "Gurgaon",
    "Noida",
    "Dwarka",
    "Saket",
    "Rohini",
    "Pitampura",
    "Lajpat Nagar",
    "Indirapuram"
]


def generate_trip(trip_number):
    pickup = random.choice(LOCATIONS)
    dropoff = random.choice(LOCATIONS)

# This will ensures that the pickup and dropoff are not the same
    while pickup == dropoff:
        dropoff = random.choice(LOCATIONS)

    distance = round(random.uniform(2, 25), 2)

    fare = round(distance * random.uniform(12, 22), 2)

    trip = {
        "trip_id": f"TRIP_{trip_number:06}",
        "driver_id": f"DRV_{random.randint(100,999)}",
        "rider_id": f"RID_{random.randint(1000,9999)}",
        "pickup_location": pickup,
        "dropoff_location": dropoff,
        "distance_km": distance,
        "fare": fare,
        "trip_status": "REQUESTED",
        "event_timestamp": datetime.now().isoformat()
    }

    return trip


if __name__ == "__main__":

    trip_number = 1

    while True:

        trip = generate_trip(trip_number)

        print(trip)

        trip_number += 1

        time.sleep(1)