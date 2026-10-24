import random
from datetime import UTC, datetime, timedelta

from hatid import simulator

T0 = datetime(2026, 10, 1, 4, 0, tzinfo=UTC)  # 12:00 Manila, lunch peak


def world(tips: bool = False) -> simulator.World:
    return simulator.World(
        customers=list(range(1, 501)),
        restaurants={i: "Makati" for i in range(1, 21)},
        ratings={},
        riders_by_city={"Makati": list(range(1, 31))},
        inactive_riders={1, 2},
        tips_enabled=tips,
        next_order_id=1,
        next_customer_id=501,
    )


def lifecycle(w):
    by_order = {}
    for when, _, kind, p in sorted(w.events):
        if "order_id" in p:
            by_order.setdefault(p["order_id"], []).append((when, kind, p))
    return by_order


def test_lifecycle_events_are_in_a_valid_order():
    w, rng = world(), random.Random(1)
    for i in range(300):
        simulator._plan_order(w, rng, T0 + timedelta(minutes=i))
    order = [
        "order_placed",
        "order_accepted",
        "delivery_assigned",
        "order_ready",
        "picked_up",
        "delivered",
    ]
    for events in lifecycle(w).values():
        kinds = [k for _, k, _ in events]
        assert kinds[0] == "order_placed"
        happy = [k for k in kinds if k in order]
        assert happy == sorted(happy, key=lambda k: order.index(k) if k != "delivery_assigned" else 1.5)
        times = [t for t, _, _ in events]
        assert times == sorted(times)


def test_inactive_riders_are_never_assigned_and_tips_need_the_feature():
    w, rng = world(tips=False), random.Random(2)
    for i in range(300):
        simulator._plan_order(w, rng, T0 + timedelta(minutes=i))
    assigned = {p["rider_id"] for _, _, k, p in w.events if k == "delivery_assigned"}
    assert assigned and not assigned & {1, 2}
    assert not any(k == "tip_added" for _, _, k, _ in w.events)

    w2, rng2 = world(tips=True), random.Random(2)
    for i in range(300):
        simulator._plan_order(w2, rng2, T0 + timedelta(minutes=i))
    tips = sum(k == "tip_added" for _, _, k, _ in w2.events)
    assert 60 < tips < 130  # ~35% of ~280 delivered orders


def test_window_planning_is_deterministic_and_peaks_at_meal_times():
    def plan(seed):
        w, rng = world(), random.Random(seed)
        simulator._plan_window(w, rng, T0 - timedelta(hours=4), T0 + timedelta(hours=20), 1500)
        return w

    a, b = plan("x"), plan("x")
    assert [(e[0], e[2]) for e in a.events] == [(e[0], e[2]) for e in b.events]
    placed = [when for when, _, k, _ in a.events if k == "order_placed"]
    by_hour = {h: sum((t + simulator.MANILA).hour == h for t in placed) for h in range(24)}
    assert by_hour[12] > 3 * by_hour[3] and by_hour[19] > 3 * by_hour[3]
