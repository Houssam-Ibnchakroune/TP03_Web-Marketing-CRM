import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import numpy as np

fake = Faker()

N_SESSIONS = 10000
CHANNELS = ['organic', 'paid', 'email', 'social']
CHANNEL_PROBA = [0.35, 0.25, 0.25, 0.15]

conn = psycopg2.connect(
    dbname="TP3_DB",
    user="postgres",
    password="0000",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

for i in range(N_SESSIONS):
    user_id = f"user_{random.randint(1, 3000):05d}"
    session_id = f"sess_{i:06d}"
    channel = random.choices(CHANNELS, CHANNEL_PROBA)[0]

    start_time = fake.date_time_between(
        start_date="-30d", end_date="now"
    )
    duration = random.randint(1, 40)
    end_time = start_time + timedelta(minutes=duration)

    pages_viewed = np.random.poisson(lam=3)
    pages_viewed = max(1, pages_viewed)

    # Probabilité de conversion
    base_p = {
        'organic': 0.15,
        'paid': 0.30,
        'email': 0.35,
        'social': 0.10
    }[channel]

    prob_conversion = min(0.9, base_p + pages_viewed * 0.03)
    converted = random.random() < prob_conversion

    # Revenue
    revenue = round(random.uniform(50, 500), 2) if converted else 0

    # Insert session
    cur.execute("""
        INSERT INTO sessions (
            session_id, user_id, start_time, end_time,
            pages_viewed, converted, channel
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        session_id, user_id, start_time, end_time,
        pages_viewed, converted, channel
    ))

    # EVENTS
    cur.execute("""
        INSERT INTO events (user_id, event_type, channel, timestamp)
        VALUES (%s, 'page_view', %s, %s)
    """, (user_id, channel, start_time))

    if pages_viewed > 2:
        cur.execute("""
            INSERT INTO events (user_id, event_type, channel, timestamp)
            VALUES (%s, 'add_to_cart', %s, %s)
        """, (user_id, channel, start_time + timedelta(minutes=5)))

    if converted:
        cur.execute("""
            INSERT INTO events (
                user_id, event_type, channel, revenue, timestamp
            ) VALUES (%s, 'purchase', %s, %s, %s)
        """, (
            user_id, channel, revenue,
            start_time + timedelta(minutes=duration)
        ))

conn.commit()
cur.close()
conn.close()

print(" 10 000 sessions et events générés avec succès")
