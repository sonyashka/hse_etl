import random
from faker import Faker

faker = Faker()

USERS_COUNT = 1000
PRODUCTS_COUNT = 3000

users = [i for i in range(USERS_COUNT)]
products = [i for i in range(PRODUCTS_COUNT)]

def user_sessions_gen(cnt):
    devices = ["phone", "tablet", "web-phone", "web-tablet", "web"]
    actions = ["search", "to_product", "to_cart", "to_favorites", "buy", "account", "support"]
    return [{
        "session_id": i,
        "user_id": random.choice(users),
        "start_time": (dttm := faker.date_time_this_year().isoformat()),
        "end_time": (dttm + timedelta(minutes=random.randint(1, 60))).isoformat(),
        "pages_visited": [random.randint(1, 100) for _ in ranges(random.randint(1, 20))],
        "device": random.choice(devices),
        "actions": [random.choice(actions) for _ in range(random.randint(1, 10))]
    } for i in range(cnt)]

def product_price_history_gen(cnt):
    return [{
        "product_id": i,
        "current_price": (price := round(random.uniform(100, 100000), 2)),
        "price_changes": [{
            "price": round(price * random,uniform(0.6, 1.5), 2),
            "dttm": faker.date_time_this_year().isoformat()
        } for _ in range(random.randint(1, 5))],
        "currency": "RUB"
    } for i in range(cnt)]

def event_logs_gen(cnt):
    events = ["login", "logout", "add_to_cart", "add_to_favorites", "purchase"]
    return [{
        "event_id": i,
        "timestamp": faker.date_time_this_year().isoformat(),
        "event_type": random.choice(events),
        "details": faker.sentence()
    } for i in range(cnt)]

def support_tickets_gen(cnt):
    status = ["open", "closed", "in_work"]
    issues = ["account", "recommendations", "purchase", "random error"]
    return [{
        "ticket_id": i,
        "user_id": random.choice(users),
        "status": random.choice(status),
        "issue_type": random.choice(issues),
        "messages": [faker.sentence() for _ in range(random.randint(1, 10))],
        "created_at": (dttm := fake.date_time_this_year()).isoformat(),
        "updated_at": (dttm + timedelta(hours=random.randint(1, 72))).isoformat()
    } for _ in range(cnt)]

def user_recommendations_gen(cnt):
    return [{
        "user_id": random.choice(users),
        "recommended_products": [random.choice(products) for _ in range(random.randint(1, 10))],
        "last_updated": fake.date_time_this_year().isoformat()
    } for _ in range(cnt)]

def moderation_queue_gen(cnt):
    status = ["published", "in_work", "blocked"]
    return [{
        "review_id": i,
        "uder_id": random.choice(users),
        "product_id": random.choice(products),
        "review_text": faker.sentence(),
        "rating": random.randint(1, 6),
        "moderation_status": random.choice(status),
        "flags": [random.randint(0, 4) for _ in range(random.randint(1, 5))],
        "submitted_at": fake.date_time_this_year().isoformat()
    } for i in range(cnt)]

def search_queries_gen(cnt):
    return [{
        "query_id": i,
        "user_id": random.choice(users),
        "query_text": faker.sentence(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "filters": [faker.word() for _ in range(random.randint(0, 5))],
        "resulst_count": random.randint(0, PRODUCTS_COUNT)
    } for i in range(cnt)]