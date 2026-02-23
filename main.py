def build_message(customer_name: str, product_name: str, message_type: str) -> str:
    if message_type == "reorder_reminder":
        return f"Merhaba {customer_name}, {product_name} ürününüz bitmek üzere olabilir. Yenilemek ister misiniz?"
    if message_type == "accessory_offer":
        return f"Merhaba {customer_name}, {product_name} için tamamlayıcı ürünlerimizi görmek ister misiniz?"
    return f"Merhaba {customer_name}, {product_name} ile ilgili yeni fırsatlarımız var. Göz atmak ister misiniz?"
import argparse
from datetime import datetime, timedelta
import pandas as pd


def parse_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, format="%Y-%m-%d", errors="raise")


def build_message_plan(
    products_csv: str,
    orders_csv: str,
    items_csv: str,
    out_csv: str,
    lead_days: int,
    durable_after_days: int,
    mid_after_days: int,
    today_str: str | None,
):
    # Load data
    products = pd.read_csv(products_csv)
    orders = pd.read_csv(orders_csv)
    items = pd.read_csv(items_csv)

    # Validate required columns
    required_products = {"product_id", "product_name", "product_type"}
    required_orders = {"order_id", "customer_name", "order_date"}
    required_items = {"order_id", "product_id", "quantity"}

    if not required_products.issubset(products.columns):
        missing = required_products - set(products.columns)
        raise ValueError(f"products.csv missing columns: {missing}")

    if not required_orders.issubset(orders.columns):
        missing = required_orders - set(orders.columns)
        raise ValueError(f"orders.csv missing columns: {missing}")

    if not required_items.issubset(items.columns):
        missing = required_items - set(items.columns)
        raise ValueError(f"order_items.csv missing columns: {missing}")

    # Parse dates
    orders["order_date"] = orders["order_date"].apply(parse_date)

    # Choose 'today'
    if today_str:
        today = parse_date(today_str).normalize()
    else:
        today = pd.Timestamp(datetime.now().date())

    # Join: orders + items + products
    df = (
        items.merge(orders, on="order_id", how="left")
             .merge(products, on="product_id", how="left")
    )

    # Basic sanity checks
    if df["customer_name"].isna().any():
        raise ValueError("Some order_id in order_items.csv not found in orders.csv")
    if df["product_name"].isna().any():
        raise ValueError("Some product_id in order_items.csv not found in products.csv")

    # Latest purchase per customer x product
    last_purchase = (
        df.groupby(["customer_name", "product_id", "product_name", "product_type"], as_index=False)
          .agg(last_order_date=("order_date", "max"))
    )

    # Helper: compute trigger date & message type
    def compute_trigger(row):
        ptype = str(row["product_type"]).strip().lower()
        last_dt = row["last_order_date"]

        if ptype == "consumable":
            # Needs shelf_life_days
            shelf = products.loc[products["product_id"] == row["product_id"], "shelf_life_days"]
            if shelf.empty or pd.isna(shelf.iloc[0]):
                # If missing shelf life, fallback to 30
                shelf_days = 30
            else:
                shelf_days = int(shelf.iloc[0])

            trigger_date = last_dt + pd.Timedelta(days=(shelf_days - lead_days))
            return trigger_date, "reorder_reminder"

        if ptype == "durable":
            trigger_date = last_dt + pd.Timedelta(days=durable_after_days)
            return trigger_date, "accessory_offer"

        # mid or unknown -> treat as mid
        trigger_date = last_dt + pd.Timedelta(days=mid_after_days)
        return trigger_date, "campaign_followup"

    triggers = last_purchase.apply(lambda r: compute_trigger(r), axis=1, result_type="expand")
    last_purchase["trigger_date"] = triggers[0]
    last_purchase["message_type"] = triggers[1]

    # Only messages that are due today or earlier
        # Only messages that are due today or earlier
    due = last_purchase[last_purchase["trigger_date"] <= today].copy()

    # Create plan (what is due)
    plan = due[["customer_name", "product_name", "product_type", "last_order_date", "trigger_date", "message_type"]].copy()
    plan = plan.sort_values(["trigger_date", "customer_name", "product_name"])

    # Save plan CSV
    plan.to_csv(out_csv, index=False)

    # Build message text + create outbox
    plan["message_text"] = plan.apply(
        lambda r: build_message(r["customer_name"], r["product_name"], r["message_type"]),
        axis=1
    )

    outbox = plan[["customer_name", "message_type", "message_text"]]
    outbox.to_csv("message_outbox.csv", index=False)

    print(f"✅ Message plan created: {out_csv}")
    print("✅ Outbox created: message_outbox.csv")
    print(f"   Today: {today.date()} | Due messages: {len(plan)}")

    # Save plan CSV
    plan.to_csv(out_csv, index=False)

    # Build message text + create outbox
   
    # Output plan
   


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a message plan for e-commerce repeat sales.")
    parser.add_argument("--products", default="data/products.csv")
    parser.add_argument("--orders", default="data/orders.csv")
    parser.add_argument("--items", default="data/order_items.csv")
    parser.add_argument("--out", default="output/message_plan.csv")

    parser.add_argument("--lead-days", type=int, default=3, help="Days before consumable shelf life to remind")
    parser.add_argument("--durable-after-days", type=int, default=7, help="Days after purchase to suggest accessories")
    parser.add_argument("--mid-after-days", type=int, default=30, help="Days after purchase to send follow-up campaign")
    parser.add_argument("--today", default=None, help="Override today date (YYYY-MM-DD) for testing")

    args = parser.parse_args()

    build_message_plan(
        products_csv=args.products,
        orders_csv=args.orders,
        items_csv=args.items,
        out_csv=args.out,
        lead_days=args.lead_days,
        durable_after_days=args.durable_after_days,
        mid_after_days=args.mid_after_days,
        today_str=args.today,
    )