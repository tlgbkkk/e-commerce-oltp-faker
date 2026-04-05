import random
import io
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from database import connect
from config import SEED

random.seed(SEED)

# Data Configuration
TOTAL_ORDERS = 2500000
BATCH_SIZE = 100000
START_DATE = datetime(2025, 8, 1)
END_DATE = datetime(2025, 10, 31)

STATUSES = ['PLACED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']
WEIGHTS = [5, 4, 11, 70, 7, 3]

def get_random_date():
    delta = END_DATE - START_DATE
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return START_DATE + timedelta(seconds=random_seconds)

def seed_transactions():
    print("Starting ingest: 2.5M orders")
    conn = connect()
    if not conn: return
    cur = conn.cursor()

    try:
        print("Loading products...")
        cur.execute("SELECT product_id, seller_id, discount_price FROM product WHERE is_active = TRUE")
        products = cur.fetchall()

        seller_products = defaultdict(list)
        for pid, sid, price in products:
            seller_products[sid].append((pid, price))
        valid_sellers = [sid for sid, prods in seller_products.items() if len(prods) > 0]

        order_id_counter = 1
        order_item_id_counter = 1

        for batch_start in range(0, TOTAL_ORDERS, BATCH_SIZE):
            orders_buffer = io.StringIO()
            items_buffer = io.StringIO()

            orders_writer = csv.writer(orders_buffer, delimiter='\t')
            items_writer = csv.writer(items_buffer, delimiter='\t')

            for _ in range(BATCH_SIZE):
                if order_id_counter > TOTAL_ORDERS: break

                seller_id = random.choice(valid_sellers)
                order_date = get_random_date()
                status = random.choices(STATUSES, weights=WEIGHTS)[0]

                available_prods = seller_products[seller_id]
                k_items = min(random.randint(2, 4), len(available_prods))
                chosen_products = random.sample(available_prods, k=k_items)

                total_amount = 0
                date_str = order_date.strftime('%Y-%m-%d %H:%M:%S')

                for pid, price in chosen_products:
                    qty = random.randint(1, 5)
                    subtotal = float(price) * qty
                    total_amount += subtotal

                    items_writer.writerow([
                        order_item_id_counter, order_id_counter, pid,
                        date_str, qty, price, subtotal
                    ])
                    order_item_id_counter += 1

                orders_writer.writerow([
                    order_id_counter, date_str, seller_id, status, total_amount
                ])
                order_id_counter += 1

            orders_buffer.seek(0)
            items_buffer.seek(0)

            copy_order_sql = """
                COPY "order" (order_id, order_date, seller_id, status, total_amount) 
                FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
            """
            cur.copy_expert(copy_order_sql, orders_buffer)

            copy_item_sql = """
                COPY order_item (order_item_id, order_id, product_id, order_date, quantity, unit_price, subtotal) 
                FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
            """
            cur.copy_expert(copy_item_sql, items_buffer)

            conn.commit()
            print(f"Batch completed: {batch_start + BATCH_SIZE}")

            orders_buffer.close()
            items_buffer.close()

        print("Syncing sequences...")
        cur.execute("SELECT setval(pg_get_serial_sequence('\"order\"', 'order_id'), COALESCE(MAX(order_id), 1) + 1, false) FROM \"order\";")
        cur.execute("SELECT setval(pg_get_serial_sequence('order_item', 'order_item_id'), COALESCE(MAX(order_item_id), 1) + 1, false) FROM order_item;")

        print("Updating stock levels...")
        cur.execute("""
            UPDATE product p
            SET stock_qty = p.stock_qty - sub.total_sold
            FROM (
                SELECT product_id, SUM(quantity) as total_sold
                FROM order_item
                GROUP BY product_id
            ) sub
            WHERE p.product_id = sub.product_id;
        """)
        conn.commit()
        print("Success: Ingest and stock update complete.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    seed_transactions()