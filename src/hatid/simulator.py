"""Workload simulator: plays the Hatid delivery app against Postgres.

Every simulated action becomes an ordinary INSERT, UPDATE or DELETE on the ``app`` schema, so
the CDC pipeline sees realistic change streams: an order row is updated four or five times over
its life, customers move city or delete their accounts (anonymising their past orders through
``ON DELETE SET NULL``), and restaurant ratings drift.

The simulator advances in windows (``simulate(hours=6)``). Lifecycle events that fall after the
window end are stored in ``sim.pending`` and applied by the next run, so repeated scheduled runs
(Dagster) produce one continuous history. Randomness is seeded per window, so a given seed and
sequence of windows always produces the same database.
"""

from __future__ import annotations

import heapq
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg

MANILA = timedelta(hours=8)  # Asia/Manila, no DST
DEFAULT_START = datetime(2026, 10, 1, 0, 0, tzinfo=UTC) - MANILA  # 1 Oct 2026, 00:00 Manila

CITIES = {
    "Quezon City": 0.24, "Makati": 0.14, "Taguig": 0.13, "Pasig": 0.11,
    "Manila": 0.14, "Cebu City": 0.14, "Davao City": 0.10,
}  # fmt: skip
CUISINES = ["Filipino", "Chicken", "Burgers", "Pizza", "Japanese", "Korean", "Milk Tea",
            "Desserts", "Chinese", "Healthy", "Coffee", "Seafood"]  # fmt: skip
FIRST = ["Ana", "Ben", "Carla", "Dan", "Ella", "Franco", "Gia", "Hans", "Isa", "Jomar", "Kat",
         "Leo", "Mika", "Nico", "Olive", "Paolo", "Rica", "Sam", "Tina", "Vince", "Yna", "Zed"]  # fmt: skip
LAST = ["Santos", "Reyes", "Cruz", "Bautista", "Garcia", "Mendoza", "Torres", "Flores",
        "Villanueva", "Ramos", "Aquino", "Castillo", "Navarro", "Dela Cruz", "Lim", "Tan"]  # fmt: skip
PLACE = ["Kusina", "Grill", "House", "Express", "Bowl", "Corner", "Kitchen", "Bites", "Hub", "Co."]
VEHICLES = {"motorcycle": 0.8, "bicycle": 0.12, "e-bike": 0.08}
# Relative order volume by local hour (lunch and dinner peaks).
HOUR_WEIGHTS = [0.2, 0.1, 0.05, 0.05, 0.05, 0.1, 0.3, 0.6, 0.8, 0.7, 0.9, 1.6, 2.0, 1.4,
                0.8, 0.7, 0.8, 1.2, 1.9, 2.0, 1.5, 1.0, 0.6, 0.4]  # fmt: skip
PEAK = {11, 12, 13, 18, 19, 20}


@dataclass
class World:
    """In-memory view of the entities that new events can refer to."""

    customers: list[int]
    restaurants: dict[int, str]  # id -> city
    ratings: dict[int, Decimal]
    riders_by_city: dict[str, list[int]]
    inactive_riders: set[int]
    tips_enabled: bool
    next_order_id: int
    next_customer_id: int
    events: list = field(default_factory=list)  # heap of (time, seq, kind, payload)
    seq: int = 0

    def push(self, when: datetime, kind: str, **payload) -> None:
        self.seq += 1
        heapq.heappush(self.events, (when, self.seq, kind, payload))


def _pick(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def _name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST)} {rng.choice(LAST)}"


# --------------------------------------------------------------------------- seeding


def seed(conn: psycopg.Connection, *, seed_value: int = 7, customers: int = 3000,
         restaurants: int = 120, riders: int = 220, start: datetime = DEFAULT_START) -> None:  # fmt: skip
    """Create the initial customers, restaurants and riders (idempotent: no-op if seeded)."""
    with conn.cursor() as cur:
        cur.execute("select count(*) from sim.state")
        if cur.fetchone()[0]:
            return
        rng = random.Random(seed_value)

        def joined(days_back: int) -> datetime:
            return start - timedelta(days=rng.randint(1, days_back), minutes=rng.randint(0, 1439))

        cust_rows = []
        for cid in range(1, customers + 1):
            name, ts = _name(rng), joined(400)
            email = f"{name.lower().replace(' ', '.')}.{cid}@example.com"
            cust_rows.append((cid, name, email, _pick(rng, CITIES), ts, ts))
        cur.executemany("insert into app.customers values (%s,%s,%s,%s,%s,%s)", cust_rows)

        rest_rows = []
        for rid in range(1, restaurants + 1):
            cuisine, city, ts = rng.choice(CUISINES), _pick(rng, CITIES), joined(700)
            name = f"{rng.choice(LAST)}'s {cuisine} {rng.choice(PLACE)}"
            rating = Decimal(str(round(rng.uniform(3.6, 4.9), 1)))
            rest_rows.append((rid, name, cuisine, city, rating, True, ts, ts))
        cur.executemany("insert into app.restaurants values (%s,%s,%s,%s,%s,%s,%s,%s)", rest_rows)

        rider_rows = []
        for rid in range(1, riders + 1):
            ts = joined(500)
            rider_rows.append((rid, _name(rng), _pick(rng, CITIES), _pick(rng, VEHICLES), True, ts, ts))
        cur.executemany("insert into app.riders values (%s,%s,%s,%s,%s,%s,%s)", rider_rows)

        cur.execute(
            "insert into sim.state (sim_clock, next_order_id, next_customer_id, seed) values (%s,1,%s,%s)",
            (start, customers + 1, seed_value),
        )
    conn.commit()


# --------------------------------------------------------------------------- planning


def _plan_order(w: World, rng: random.Random, t0: datetime) -> None:
    """Schedule one order's whole lifecycle as future events."""
    order_id = w.next_order_id
    w.next_order_id += 1
    restaurant_id = rng.choice(list(w.restaurants))
    city = w.restaurants[restaurant_id]
    customer_id = w.customers[min(int(rng.paretovariate(1.2)) - 1, len(w.customers) - 1)
                              if rng.random() < 0.3 else rng.randrange(len(w.customers))]  # fmt: skip
    items = rng.choices([1, 2, 3, 4], weights=[4, 4, 2, 1])[0]
    subtotal = Decimal(str(round(sum(rng.uniform(90, 420) for _ in range(items)), 2)))
    distance = Decimal(str(round(rng.uniform(0.8, 7.5), 2)))
    fee = Decimal(str(round(39 + 8 * float(distance)))) + Decimal("0.00")
    w.push(t0, "order_placed", order_id=order_id, customer_id=customer_id,
           restaurant_id=restaurant_id, subtotal=str(subtotal), fee=str(fee))  # fmt: skip

    if rng.random() < 0.02:
        w.push(t0 + timedelta(minutes=rng.uniform(1, 4)), "order_cancelled",
               order_id=order_id, reason="restaurant_rejected")  # fmt: skip
        return
    accepted = t0 + timedelta(minutes=rng.uniform(0.5, 3))
    w.push(accepted, "order_accepted", order_id=order_id)
    if rng.random() < 0.02:
        w.push(accepted + timedelta(minutes=rng.uniform(0.2, 0.9)), "order_cancelled",
               order_id=order_id, reason="customer_cancelled")  # fmt: skip
        return

    hour = (t0 + MANILA).hour
    prep = max(5.0, rng.gauss(18, 5) if hour in PEAK else rng.gauss(13, 4))
    ready = accepted + timedelta(minutes=prep)
    w.push(ready, "order_ready", order_id=order_id)

    riders = [r for r in w.riders_by_city.get(city, []) if r not in w.inactive_riders]
    rider_id = rng.choice(riders or [r for rs in w.riders_by_city.values() for r in rs])
    assigned = accepted + timedelta(minutes=rng.uniform(1, 6))
    promised = t0 + timedelta(minutes=35 + 2 * float(distance))
    w.push(assigned, "delivery_assigned", order_id=order_id, rider_id=rider_id,
           distance=str(distance), promised=promised.isoformat())  # fmt: skip
    picked = max(ready, assigned + timedelta(minutes=rng.uniform(4, 12)))
    picked += timedelta(minutes=rng.uniform(0, 3))
    w.push(picked, "picked_up", order_id=order_id)
    if rng.random() < 0.01:
        w.push(picked + timedelta(minutes=rng.uniform(10, 30)), "delivery_failed", order_id=order_id)
        return
    traffic = 1.3 if hour in PEAK else 1.0
    delivered = picked + timedelta(minutes=float(distance) * rng.uniform(2.5, 4.5) * traffic + 2)
    w.push(delivered, "delivered", order_id=order_id)
    if w.tips_enabled and rng.random() < 0.35:
        tip = Decimal(rng.choice([20, 30, 40, 50, 60, 80, 100])) + Decimal("0.00")
        w.push(delivered + timedelta(minutes=rng.uniform(2, 30)), "tip_added",
               order_id=order_id, tip=str(tip))  # fmt: skip


def _plan_window(w: World, rng: random.Random, start: datetime, end: datetime,
                 orders_per_day: int) -> None:  # fmt: skip
    total_w = sum(HOUR_WEIGHTS)
    t = start
    while t < end:
        hour_end = min(end, t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        share = (hour_end - t) / timedelta(hours=1)
        expected = orders_per_day * HOUR_WEIGHTS[(t + MANILA).hour] / total_w * share
        n = int(expected) + (rng.random() < expected - int(expected))
        for _ in range(n):
            _plan_order(w, rng, t + (hour_end - t) * rng.random())
        t = hour_end

    days = (end - start) / timedelta(days=1)
    span = (end - start).total_seconds()

    def at() -> datetime:
        return start + timedelta(seconds=rng.uniform(0, span))

    for _ in range(round(25 * days)):  # sign-ups
        w.push(at(), "customer_signup")
    for _ in range(round(15 * days)):  # moved city
        w.push(at(), "customer_moved")
    for _ in range(round(4 * days)):  # account deletions
        w.push(at(), "customer_deleted")
    for _ in range(round(20 * days)):  # rating drift
        w.push(at(), "rating_changed")
    for _ in range(round(3 * days)):  # rider goes inactive / comes back
        w.push(at(), "rider_toggled")


# --------------------------------------------------------------------------- applying


def _apply(cur: psycopg.Cursor, w: World, rng: random.Random, when: datetime, kind: str,
           p: dict) -> None:  # fmt: skip
    oid = p.get("order_id")
    if kind == "order_placed":
        cur.execute(
            "insert into app.orders (order_id, customer_id, restaurant_id, status, subtotal,"
            " delivery_fee, placed_at, updated_at) values (%s,%s,%s,'placed',%s,%s,%s,%s)",
            (oid, p["customer_id"] if p["customer_id"] in w.customers else None,
             p["restaurant_id"], p["subtotal"], p["fee"], when, when),
        )  # fmt: skip
    elif kind == "order_accepted":
        cur.execute("update app.orders set status='accepted', accepted_at=%s, updated_at=%s"
                    " where order_id=%s and status='placed'", (when, when, oid))  # fmt: skip
    elif kind == "order_ready":
        cur.execute("update app.orders set status='ready', ready_at=%s, updated_at=%s"
                    " where order_id=%s and status='accepted'", (when, when, oid))  # fmt: skip
    elif kind == "order_cancelled":
        cur.execute("update app.orders set status='cancelled', cancelled_at=%s, cancel_reason=%s,"
                    " updated_at=%s where order_id=%s and status not in ('delivered','cancelled')",
                    (when, p["reason"], when, oid))  # fmt: skip
    elif kind == "delivery_assigned":
        cur.execute(
            "insert into app.deliveries (delivery_id, order_id, rider_id, status, distance_km,"
            " promised_at, assigned_at, updated_at) select %s,%s,%s,'assigned',%s,%s,%s,%s"
            " where exists (select 1 from app.orders where order_id=%s and status<>'cancelled')",
            (oid, oid, p["rider_id"], p["distance"], p["promised"], when, when, oid),
        )
    elif kind == "picked_up":
        cur.execute("update app.deliveries set status='picked_up', picked_up_at=%s, updated_at=%s"
                    " where order_id=%s and status='assigned'", (when, when, oid))  # fmt: skip
        cur.execute("update app.orders set status='picked_up', updated_at=%s"
                    " where order_id=%s and status in ('accepted','ready')", (when, oid))  # fmt: skip
    elif kind == "delivered":
        cur.execute("update app.deliveries set status='delivered', delivered_at=%s, updated_at=%s"
                    " where order_id=%s and status='picked_up'", (when, when, oid))  # fmt: skip
        cur.execute("update app.orders set status='delivered', updated_at=%s"
                    " where order_id=%s and status='picked_up'", (when, oid))  # fmt: skip
    elif kind == "delivery_failed":
        cur.execute("update app.deliveries set status='failed', updated_at=%s"
                    " where order_id=%s and status='picked_up'", (when, oid))  # fmt: skip
        cur.execute("update app.orders set status='cancelled', cancelled_at=%s,"
                    " cancel_reason='delivery_failed', updated_at=%s"
                    " where order_id=%s and status='picked_up'", (when, when, oid))  # fmt: skip
    elif kind == "tip_added":
        cur.execute("update app.orders set tip_amount=%s, updated_at=%s"
                    " where order_id=%s and status='delivered'", (p["tip"], when, oid))  # fmt: skip
    elif kind == "customer_signup":
        cid = w.next_customer_id
        w.next_customer_id += 1
        name = _name(rng)
        cur.execute("insert into app.customers values (%s,%s,%s,%s,%s,%s)",
                    (cid, name, f"{name.lower().replace(' ', '.')}.{cid}@example.com",
                     _pick(rng, CITIES), when, when))  # fmt: skip
        w.customers.append(cid)
    elif kind == "customer_moved" and w.customers:
        cur.execute("update app.customers set city=%s, updated_at=%s where customer_id=%s",
                    (_pick(rng, CITIES), when, rng.choice(w.customers)))  # fmt: skip
    elif kind == "customer_deleted" and len(w.customers) > 100:
        cid = w.customers.pop(rng.randrange(len(w.customers)))
        cur.execute("delete from app.customers where customer_id=%s", (cid,))
    elif kind == "rating_changed":
        rid = rng.choice(list(w.restaurants))
        new = min(
            Decimal("5.0"),
            max(Decimal("3.0"), w.ratings[rid] + rng.choice([-1, 1]) * Decimal("0.1")),
        )
        w.ratings[rid] = new
        cur.execute("update app.restaurants set rating=%s, updated_at=%s where restaurant_id=%s",
                    (new, when, rid))  # fmt: skip
    elif kind == "rider_toggled":
        rid = rng.choice([r for rs in w.riders_by_city.values() for r in rs])
        active = rid in w.inactive_riders
        (w.inactive_riders.discard if active else w.inactive_riders.add)(rid)
        cur.execute("update app.riders set is_active=%s, updated_at=%s where rider_id=%s",
                    (active, when, rid))  # fmt: skip


def _load_world(cur: psycopg.Cursor) -> tuple[World, datetime, int]:
    cur.execute("select sim_clock, next_order_id, next_customer_id, seed from sim.state")
    clock, next_order, next_customer, seed_value = cur.fetchone()
    cur.execute("select customer_id from app.customers order by customer_id")
    customers = [r[0] for r in cur.fetchall()]
    cur.execute("select restaurant_id, city, rating from app.restaurants order by restaurant_id")
    rest = cur.fetchall()
    cur.execute("select rider_id, city, is_active from app.riders order by rider_id")
    riders_by_city: dict[str, list[int]] = {}
    inactive: set[int] = set()
    for rid, city, active in cur.fetchall():
        riders_by_city.setdefault(city, []).append(rid)
        if not active:
            inactive.add(rid)
    cur.execute("select 1 from information_schema.columns where table_schema='app'"
                " and table_name='orders' and column_name='tip_amount'")  # fmt: skip
    w = World(customers, {r[0]: r[1] for r in rest}, {r[0]: r[2] for r in rest},
              riders_by_city, inactive, cur.fetchone() is not None, next_order, next_customer)  # fmt: skip
    cur.execute("select event_time, seq, kind, payload from sim.pending order by event_time, seq")
    for when, _, kind, payload in cur.fetchall():
        w.push(when, kind, **payload)
    return w, clock, seed_value


@dataclass(frozen=True)
class WindowStats:
    start: datetime
    end: datetime
    applied: int
    pending: int
    orders_placed: int


def simulate(conn: psycopg.Connection, *, hours: float = 6, orders_per_day: int = 1500,
             batch_size: int = 500) -> WindowStats:  # fmt: skip
    """Advance the simulation clock by ``hours`` and write the resulting changes."""
    with conn.cursor() as cur:
        w, start, seed_value = _load_world(cur)
        end = start + timedelta(hours=hours)
        rng = random.Random(f"{seed_value}:{start.isoformat()}")
        first_order = w.next_order_id
        _plan_window(w, rng, start, end, orders_per_day)

        applied = 0
        while w.events and w.events[0][0] < end:
            when, _, kind, payload = heapq.heappop(w.events)
            _apply(cur, w, rng, when, kind, payload)
            applied += 1
            if applied % batch_size == 0:
                conn.commit()  # many small transactions, like a real app

        cur.execute("delete from sim.pending")
        cur.executemany(
            "insert into sim.pending values (%s,%s,%s,%s)",
            [(when, seq, kind, json.dumps(p)) for when, seq, kind, p in w.events],
        )
        cur.execute("update sim.state set sim_clock=%s, next_order_id=%s, next_customer_id=%s",
                    (end, w.next_order_id, w.next_customer_id))  # fmt: skip
    conn.commit()
    return WindowStats(start, end, applied, len(w.events), w.next_order_id - first_order)


def enable_tips(conn: psycopg.Connection) -> None:
    """Source side of the live schema change: the app starts storing tips."""
    with conn.cursor() as cur:
        cur.execute("alter table app.orders add column if not exists tip_amount numeric(10, 2)")
    conn.commit()
