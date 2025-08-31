import os
from re import A
import sys
import argparse
import csv
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Optional

import psycopg2
from psycopg2.errors import ProhibitedSqlStatementAttempted
from psycopg2.extras import execute_values

from dotenv import load_dotenv
load_dotenv()

# Import shared database utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import get_connection, parse_decimal


def read_product_csv(csv_path: str) -> List[Dict]:
    """
    Read product CSV file and return list of product dictionaries.
    Expected columns: product_id, product_name, source, unit, cost_per_unit (optional)
    """
    required_cols = {"product_id", "product_name"}
    rows = []
    
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")
        
        header = {h.strip() for h in reader.fieldnames}
        missing = required_cols - header
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")
        
        for r in reader:
            # Clean and prepare row data
            row_data = {}
            for k, v in r.items():
                key = k.strip()
                value = v.strip() if isinstance(v, str) else v
                row_data[key] = value if value != "" else None
            
            # Validate product_id
            try:
                row_data["product_id"] = int(row_data["product_id"])
            except (ValueError, TypeError):
                print(f"WARNING: Invalid product_id '{row_data.get('product_id')}', skipping row")
                continue
            
            # Parse cost_per_unit if present
            if "cost_per_unit" in row_data and row_data["cost_per_unit"]:
                row_data["cost_per_unit"] = parse_decimal(row_data["cost_per_unit"])
            
            rows.append(row_data)
    
    return rows


def upsert_product(conn, product_data: Dict) -> None:
    """
    Insert or update product in the product table.
    """
    with conn.cursor() as cur:
        # Use ON CONFLICT to handle upserts
        cur.execute("""
            INSERT INTO dbo.product (product_id, product_name, source, unit)
            VALUES (%(product_id)s, %(product_name)s, %(source)s, %(unit)s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                source = EXCLUDED.source,
                unit = EXCLUDED.unit
        """, {
            "product_id": product_data["product_id"],
            "product_name": product_data["product_name"],
            "source": product_data.get("source"),
            "unit": product_data.get("unit")
        })


def insert_product_price(conn, product_id: int, cost_per_unit: Decimal, created_when: datetime = None) -> None:
    """
    Insert a new product price record.
    """
    if created_when is None:
        created_when = datetime.now()
    
    with conn.cursor() as cur:
        # Check if there's already a price for this product at this exact timestamp
        cur.execute("""
            SELECT cost_per_unit FROM product_price 
            WHERE product_id = %s AND created_when = %s
        """, (product_id, created_when))
        
        existing = cur.fetchone()
        if existing:
            # Update existing price if different
            if existing[0] != cost_per_unit:
                cur.execute("""
                    UPDATE product_price 
                    SET cost_per_unit = %s 
                    WHERE product_id = %s AND created_when = %s
                """, (cost_per_unit, product_id, created_when))
                return True
            return False
        else:
            # Insert new price record
            cur.execute("""
                INSERT INTO dbo.product_price (product_id, cost_per_unit, created_when)
                VALUES (%s, %s, %s)
            """, (product_id, cost_per_unit, created_when))
            return True


def get_latest_product_price(conn, product_id: int) -> Optional[Decimal]:
    """
    Get the latest price for a product.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cost_per_unit FROM product_price 
            WHERE product_id = %s 
            ORDER BY created_when DESC 
            LIMIT 1
        """, (product_id,))
        
        result = cur.fetchone()
        return result[0] if result else None


def import_product_csv(
    csv_path: str,
    update_prices: bool = True,
    price_timestamp: datetime = None
) -> None:
    """
    Import products from CSV file.
    
    Args:
        csv_path: Path to CSV file
        update_prices: Whether to update/insert prices if cost_per_unit is provided
        price_timestamp: Timestamp for price records (defaults to now)
    """
    products = read_product_csv(csv_path)
    
    if not products:
        print("No valid product data found in CSV")
        return
    
    conn = get_connection()
    try:
        with conn:
            products_updated = 0
            prices_updated = 0
            prices_inserted = 0
            
            for product in products:
                product_id = product["product_id"]
                
                # Upsert product
                upsert_product(conn, product)
                products_updated += 1
                
                # Handle price update if cost_per_unit is provided
                if update_prices and "cost_per_unit" in product and product["cost_per_unit"] is not None:
                    new_price = product["cost_per_unit"]
                    
                    # Check if price has changed
                    current_price = get_latest_product_price(conn, product_id)
                    
                    if current_price is None or current_price != new_price:
                        # Insert new price record
                        price_updated = insert_product_price(
                            conn, 
                            product_id, 
                            new_price, 
                            price_timestamp
                        )
                        if price_updated:
                            if current_price is None:
                                prices_inserted += 1
                                print(f"  Product {product_id}: New price {new_price}")
                            else:
                                prices_updated += 1
                                print(f"  Product {product_id}: Price updated from {current_price} to {new_price}")
                    else:
                        print(f"  Product {product_id}: Price unchanged ({new_price})")
            
            conn.commit()
            
            print(f"\nImport Summary:")
            print(f"  Products processed: {products_updated}")
            print(f"  New prices inserted: {prices_inserted}")
            print(f"  Prices updated: {prices_updated}")
            
    except Exception as e:
        conn.rollback()
        print(f"ERROR during import: {e}")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Import products from CSV into product and product_price tables."
    )
    parser.add_argument(
        "--file", 
        required=True, 
        help="Path to CSV file with columns: product_id, product_name, source, unit, cost_per_unit (optional)"
    )
    
    args = parser.parse_args()
    
    try:
        import_product_csv(
            csv_path=args.file,
            update_prices=True,
            price_timestamp=datetime.now()
        )
        print("Import completed successfully!")
    except Exception as e:
        import traceback
        print(f"Import failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
