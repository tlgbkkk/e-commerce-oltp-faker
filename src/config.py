from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "user": "postgres",
    "password": "170723",
    "database": "ecommerce_oltp",
    "port": 5432,
}

SEED = 42

# MASTER CONFIGURATION

DATA_VOLUME = {
    "brand": 20,
    "category": 10,
    "seller": 25,
    "product": 2000,
    "promotion": 10,
    "promotion_product": 100
}

CATEGORY_MAP = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Accessories", "Cameras"],
    "Fashion": ["Clothing", "Shoes", "Bags", "Jewelry", "Watches"],
    "Home Appliances": ["Kitchenware", "Washing Machines", "Air Purifiers", "Fridges"],
    "Beauty": ["Skincare", "Makeup", "Perfume", "Hair Care"],
    "Sports": ["Gym Gear", "Outdoor Equipment", "Yoga Mats", "Cycling"]
}

PROMO_NAMES = ["Mega Sale", "Flash Deal", "Black Friday", "Birthday", "Holiday"]
PROMO_TYPES = ["Seasonal", "Flash Sale", "Member Only", "Voucher Code"]

# TRANSACTION CONFIGURATION

TOTAL_ORDERS = 2500000
BATCH_SIZE = 100000
START_DATE = datetime(2025, 8, 1)
END_DATE = datetime(2025, 10, 31)

STATUSES = ['PLACED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']
WEIGHTS = [5, 4, 11, 70, 7, 3]