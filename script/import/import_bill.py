import os
import sys
import argparse
import csv
from decimal import Decimal
from datetime import date

import psycopg2

# Import shared database utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import get_connection, parse_decimal


def read_bill_csv(path: str):
    required_cols = {"product_name", "quantity", "product_price", "tax_amount", "total"}
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")
        header = {h.strip() for h in reader.fieldnames}
        missing = required_cols - header
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")
        for r in reader:
            rows.append({k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    return rows


def import_bill(
    csv_path: str,
    vendor_name: str | None,
    notes: str | None,
    shipping_amount: Decimal,
    currency: str,
    bill_number: str | None,
    bill_date: date | None,
    source: str | None,
):
    items = read_bill_csv(csv_path)

    subtotal = Decimal("0")
    tax_sum = Decimal("0")

    # Prepare line items
    prepared_items = []
    for r in items:
        product_name = r.get("product_name")
        quantity = parse_decimal(r.get("quantity"))
        unit_price = parse_decimal(r.get("product_price"))
        tax_amount = parse_decimal(r.get("tax_amount"))
        line_total = parse_decimal(r.get("total"))

        subtotal += (quantity * unit_price)
        tax_sum += tax_amount

        prepared_items.append(
            {
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )

    total_amount = subtotal + tax_sum + shipping_amount

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Insert bill header
                cur.execute(
                    """
                    INSERT INTO bill (
                        bill_number, vendor_name, source, bill_date, currency,
                        subtotal_amount, tax_amount, shipping_amount, total_amount, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING bill_id
                    """,
                    (
                        bill_number,
                        vendor_name,
                        source if source else csv_path,
                        bill_date,
                        currency,
                        subtotal,
                        tax_sum,
                        shipping_amount,
                        total_amount,
                        notes,
                    ),
                )
                bill_id = cur.fetchone()[0]

                # Insert bill items
                for it in prepared_items:
                    cur.execute(
                        """
                        INSERT INTO bill_item (
                            bill_id, product_id, product_name, quantity, unit, unit_price, line_total
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            bill_id,
                            None,  # product_id unknown at import time
                            it["product_name"],
                            it["quantity"],
                            None,  # unit unknown in this CSV
                            it["unit_price"],
                            it["line_total"],
                        ),
                    )
        print(
            "Imported bill with %d item(s). Subtotal=%s Tax=%s Shipping=%s Total=%s"
            % (len(prepared_items), str(subtotal), str(tax_sum), str(shipping_amount), str(total_amount))
        )
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Import a bill CSV into bill and bill_item tables.")
    parser.add_argument("--file", required=True, help="Path to CSV with columns: product_name, quantity, product_price, tax_amount, total")
    parser.add_argument("--vendor-name", required=False, default=None)
    parser.add_argument("--notes", required=False, default=None)
    parser.add_argument("--shipping-amount", required=False, default="0")
    parser.add_argument("--currency", required=False, default="USD")
    parser.add_argument("--bill-number", required=False, default=None)
    parser.add_argument("--bill-date", required=False, default=None, help="YYYY-MM-DD")
    parser.add_argument("--source", required=False, default=None, help="Bill source URL or path")

    args = parser.parse_args()

    shipping_amount = parse_decimal(args.shipping_amount)
    currency = (args.currency or "USD").upper()[:3]

    bdate = None
    if args.bill_date:
        try:
            y, m, d = [int(x) for x in args.bill_date.split("-")]
            bdate = date(y, m, d)
        except Exception:
            print("WARNING: Invalid --bill-date; ignoring.")
            bdate = None

    import_bill(
        csv_path=args.file,
        vendor_name=args.vendor_name,
        notes=args.notes,
        shipping_amount=shipping_amount,
        currency=currency,
        bill_number=args.bill_number,
        bill_date=bdate,
        source=args.source,
    )


if __name__ == "__main__":
    main()


