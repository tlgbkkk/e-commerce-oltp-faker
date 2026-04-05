import random
from faker import Faker
import psycopg2
import psycopg2.extras
from database import connect
from config import SEED, DATA_VOLUME, CATEGORY_MAP, PROMO_NAMES, PROMO_TYPES

fake = Faker()
fake_vi = Faker('vi_VN')
Faker.seed(SEED)
random.seed(SEED)


def generate_and_ingest_data():
    print("Generating data...")

    brand_data = [
        (i, fake.company(), fake.country(), fake.date_time_this_decade())
        for i in range(1, DATA_VOLUME['brand'] + 1)
    ]

    category_data = []
    current_id = 1
    parent_ids = {}
    for p_name in CATEGORY_MAP.keys():
        category_data.append((current_id, p_name, None, 1, fake.date_time_this_year()))
        parent_ids[p_name] = current_id
        current_id += 1

    all_children = []
    for p_name, children in CATEGORY_MAP.items():
        for child in children:
            all_children.append((child, parent_ids[p_name]))

    random.shuffle(all_children)

    for child_name, p_id in all_children:
        if len(category_data) >= DATA_VOLUME['category']:
            break

        category_data.append((current_id, child_name, p_id, 2, fake.date_time_this_year()))
        current_id += 1

    seller_data = [
        (i, random.choice([fake.company(), fake_vi.name()]), fake.date_between(start_date='-3y', end_date='today'),
         random.choice(["Official", "Marketplace"]), round(random.uniform(0.0, 5.0), 1), 'Vietnam')
        for i in range(1, DATA_VOLUME['seller'] + 1)
    ]

    promotion_data = []
    promo_dict = {}

    for i in range(1, DATA_VOLUME['promotion'] + 1):
        d_type = random.choice(['percentage', 'fixed_amount'])

        if d_type == 'percentage':
            d_val = random.randint(2, 50)
        else:
            d_val = round(random.uniform(10000, 500000), -3)

        promotion_data.append((
            i, f"{random.choice(PROMO_NAMES)} {fake.month_name()}",
            random.choice(PROMO_TYPES), d_type, d_val,
            fake.date_time_between(start_date="-30d", end_date="now"),
            fake.date_time_between(start_date="now", end_date="+30d")
        ))
        promo_dict[i] = (d_type, d_val)

    promo_product_data = []
    product_promo_map = {}
    mapping_id = 1

    for i in range(1, DATA_VOLUME['promotion'] + 1):
        p_prod = random.randint(1, DATA_VOLUME['product'])
        promo_product_data.append((mapping_id, i, p_prod, fake.date_time_this_year()))

        if p_prod not in product_promo_map:
            product_promo_map[p_prod] = []
        product_promo_map[p_prod].append(i)
        mapping_id += 1

    while mapping_id <= DATA_VOLUME['promotion_product']:
        p_promo = random.randint(1, DATA_VOLUME['promotion'])
        p_prod = random.randint(1, DATA_VOLUME['product'])

        if p_prod not in product_promo_map or p_promo not in product_promo_map[p_prod]:
            promo_product_data.append((mapping_id, p_promo, p_prod, fake.date_time_this_year()))

            if p_prod not in product_promo_map:
                product_promo_map[p_prod] = []
            product_promo_map[p_prod].append(p_promo)
            mapping_id += 1

    product_data = []

    for i in range(1, DATA_VOLUME['product'] + 1):
        price = round(random.uniform(100000, 20000000), -3)
        discount_price = price

        if i in product_promo_map:
            best_price = price

            for promo_id in product_promo_map[i]:
                d_type, d_val = promo_dict[promo_id]

                if d_type == 'percentage':
                    calc_price = price * (1 - d_val / 100)
                else:  # fixed_amount
                    calc_price = price - d_val

                calc_price = max(calc_price, price * 0.1)

                if calc_price < best_price:
                    best_price = calc_price

            discount_price = round(best_price, -3)

        product_data.append((
            i, f"{fake.word().capitalize()} {fake.word().capitalize()}",
            random.randint(1, DATA_VOLUME['category']), random.randint(1, DATA_VOLUME['brand']), random.randint(1, DATA_VOLUME['seller']),
            price, discount_price,
            random.randint(0, 500), round(random.uniform(0.0, 5.0), 1),
            fake.date_time_between(start_date='-2y', end_date='now'), True
        ))

    print("Ingesting data into PostgreSQL...")
    conn = connect()
    if not conn: return
    cur = conn.cursor()
    try:
        queries = {
            "brand": "INSERT INTO brand VALUES %s",
            "category": "INSERT INTO category VALUES %s",
            "seller": "INSERT INTO seller VALUES %s",
            "product": "INSERT INTO product VALUES %s",
            "promotion": "INSERT INTO promotion VALUES %s",
            "promo_prod": "INSERT INTO promotion_product VALUES %s"
        }

        psycopg2.extras.execute_values(cur, queries["brand"], brand_data)
        psycopg2.extras.execute_values(cur, queries["category"], category_data)
        psycopg2.extras.execute_values(cur, queries["seller"], seller_data)
        psycopg2.extras.execute_values(cur, queries["product"], product_data)
        psycopg2.extras.execute_values(cur, queries["promotion"], promotion_data)
        psycopg2.extras.execute_values(cur, queries["promo_prod"], promo_product_data)

        conn.commit()
        print("All done! All data have been ingested.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    generate_and_ingest_data()