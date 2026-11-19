"""Database Seeding Script for OrderFlow Platform.

Generates realistic, interconnected e-commerce synthetic data covering customers,
catalog, sellers, warehouses, orders, line items, payments, shipments, inventory, and clickstream.
"""

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from config.logging_config import setup_logger
from src.orderflow.database.connection import db_manager
from src.orderflow.database.destination_writer import DestinationWriter
from src.orderflow.generator.customers import generate_customers
from src.orderflow.generator.inventory import generate_inventory
from src.orderflow.generator.orders import generate_orders_and_items
from src.orderflow.generator.payments import generate_payments
from src.orderflow.generator.products import generate_products
from src.orderflow.generator.sellers import generate_sellers
from src.orderflow.generator.shipments import generate_shipments
from src.orderflow.generator.warehouses import generate_warehouses
from src.orderflow.generator.web_events import generate_web_events

logger = setup_logger("orderflow.seed")


def parse_args():
    parser = argparse.ArgumentParser(description="Seed OrderFlow Operational Database.")
    parser.add_argument(
        "--profile",
        choices=["small", "standard"],
        default="small",
        help="Dataset profile size (small or standard)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate/reset operational tables before seeding",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate records and display summary without writing to database",
    )
    return parser.parse_args()


def seed_database(profile: str = "small", seed: int = 42, reset: bool = False, dry_run: bool = False):
    """Executes the synthetic data generation and database population."""
    logger.info(f"Starting database seed: profile={profile}, seed={seed}, reset={reset}, dry_run={dry_run}")

    # Set profile targets
    if profile == "small":
        counts = {
            "customers": 1500,
            "products": 250,
            "sellers": 40,
            "warehouses": 6,
            "orders": 5000,
            "inventory": 1500,
            "web_events": 25000,
        }
    else:
        counts = {
            "customers": 10000,
            "products": 1500,
            "sellers": 200,
            "warehouses": 8,
            "orders": 50000,
            "inventory": 10000,
            "web_events": 250000,
        }

    # Step 1: Generate foundational entities
    logger.info("Generating customers...")
    customers = generate_customers(count=counts["customers"], seed=seed)

    logger.info("Generating product catalog...")
    products = generate_products(count=counts["products"], seed=seed)

    logger.info("Generating sellers...")
    sellers = generate_sellers(count=counts["sellers"], seed=seed)

    logger.info("Generating warehouses...")
    warehouses = generate_warehouses(count=counts["warehouses"], seed=seed)

    # Step 2: Generate orders and order items
    logger.info("Generating orders and items...")
    orders, order_items = generate_orders_and_items(
        customers=customers,
        products=products,
        sellers=sellers,
        order_count=counts["orders"],
        seed=seed,
    )

    # Step 3: Generate payments
    logger.info("Generating payments...")
    payments = generate_payments(orders=orders, seed=seed)

    # Step 4: Generate shipments
    logger.info("Generating shipments...")
    shipments = generate_shipments(
        orders=orders,
        sellers=sellers,
        warehouses=warehouses,
        seed=seed,
    )

    # Step 5: Generate inventory
    logger.info("Generating inventory...")
    inventory = generate_inventory(
        products=products,
        warehouses=warehouses,
        target_count=counts["inventory"],
        seed=seed,
    )

    # Step 6: Generate clickstream web events
    logger.info("Generating web events...")
    web_events = generate_web_events(
        customers=customers,
        products=products,
        orders=orders,
        target_count=counts["web_events"],
        seed=seed,
    )

    summary = {
        "customers": len(customers),
        "products": len(products),
        "sellers": len(sellers),
        "warehouses": len(warehouses),
        "orders": len(orders),
        "order_items": len(order_items),
        "payments": len(payments),
        "shipments": len(shipments),
        "inventory": len(inventory),
        "web_events": len(web_events),
    }

    logger.info("Data Generation Summary:")
    for entity, count in summary.items():
        logger.info(f"  - {entity}: {count:,} records")

    if dry_run:
        logger.info("Dry run requested. Skipping database writes.")
        return summary

    # Step 7: Write to Database
    writer = DestinationWriter(manager=db_manager)
    is_sqlite = db_manager.db_url.startswith("sqlite")
    schema_prefix = None if is_sqlite else "app"

    # Execute migrations if tables don't exist yet
    migrations_dir = Path("sql/migrations")
    if migrations_dir.exists() and not is_sqlite:
        with db_manager.connect() as conn:
            tables_exist = conn.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'customers')")
            ).scalar()
        if not tables_exist:
            logger.info("Ensuring database migrations are up to date...")
            try:
                writer.execute_migrations_dir(migrations_dir)
            except Exception as e:
                logger.warning(f"Note on migration execution: {e}")
        else:
            logger.info("Database schemas and tables already exist. Skipping migration execution.")

    # Handle table reset if requested
    if reset:
        logger.info("Resetting existing operational data...")
        tables_in_order = [
            "web_events", "payments", "shipments", "order_items",
            "orders", "inventory", "warehouses", "sellers", "products", "customers"
        ]
        with db_manager.connect() as conn:
            for tbl in tables_in_order:
                target = f"{tbl}" if is_sqlite else f"app.{tbl}"
                try:
                    conn.execute(text(f"DELETE FROM {target}"))
                except Exception as e:
                    logger.warning(f"Could not clear {target}: {e}")
            conn.commit()

    # Insert DataFrames in dependency order
    dfs_to_write = [
        ("customers", pd.DataFrame(customers)),
        ("products", pd.DataFrame(products)),
        ("sellers", pd.DataFrame(sellers)),
        ("warehouses", pd.DataFrame(warehouses)),
        ("orders", pd.DataFrame(orders)),
        ("order_items", pd.DataFrame(order_items)),
        ("payments", pd.DataFrame(payments)),
        ("shipments", pd.DataFrame(shipments)),
        ("inventory", pd.DataFrame(inventory)),
        ("web_events", pd.DataFrame(web_events)),
    ]

    for table_name, df in dfs_to_write:
        logger.info(f"Writing {len(df):,} rows to {table_name}...")
        writer.write_dataframe(df, schema=schema_prefix, table=table_name, if_exists="append")

    logger.info("Database seeding completed successfully.")
    return summary


def main():
    args = parse_args()
    seed_database(
        profile=args.profile,
        seed=args.seed,
        reset=args.reset,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
