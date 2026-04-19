import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

# ── VENDORS ──────────────────────────────────────────
vendors = []
categories = ["Software", "Marketing", "Logistics", "Office", "Legal"]
methods    = ["ACH", "Card", "Wire"]
risk_tiers = ["Low", "Medium", "High"]

for i in range(1, 51):
    vendors.append({
        "vendor_id":      f"V{i:04d}",
        "vendor_name":    fake.company(),
        "category":       random.choice(categories),
        "payment_method": random.choice(methods),
        "risk_tier":      random.choice(risk_tiers)
    })

# ── INVOICES ─────────────────────────────────────────
invoices = []
statuses = ["paid", "paid", "paid", "pending", "overdue", "void"]

for i in range(1, 501):
    vendor  = random.choice(vendors)
    created = fake.date_between(start_date="-180d", end_date="-1d")
    terms   = random.choice([15, 30, 45, 60])
    invoices.append({
        "invoice_id":     f"INV{i:06d}",
        "vendor_id":      vendor["vendor_id"],
        "amount":         round(random.uniform(500, 75000), 2),
        "currency":       "USD",
        "status":         random.choice(statuses),
        "created_date":   created,
        "due_date":       created + timedelta(days=terms),
        "payment_terms":  f"NET{terms}",
        "category":       vendor["category"]
    })

# ── PAYMENTS ─────────────────────────────────────────
payments = []
paid_invoices = [inv for inv in invoices if inv["status"] == "paid"]

for i, inv in enumerate(paid_invoices):
    payments.append({
        "payment_id":   f"PAY{i:06d}",
        "invoice_id":   inv["invoice_id"],
        "amount":       inv["amount"],
        "method":       random.choice(["ACH", "Card", "Wire", "International"]),
        "status":       random.choice(["completed","completed","completed","failed"]),
        "processed_at": inv["due_date"] - timedelta(days=random.randint(0, 5)),
        "reconciled":   random.choice([True, True, False])
    })

# ── APPROVALS ────────────────────────────────────────
approvals = []
for i, inv in enumerate(invoices[:300]):
    approvals.append({
        "approval_id":  f"APR{i:06d}",
        "invoice_id":   inv["invoice_id"],
        "approver_id":  f"EMP{random.randint(1,20):03d}",
        "status":       random.choice(["approved","approved","rejected"]),
        "approved_at":  inv["created_date"] + timedelta(days=random.randint(1,3)),
        "threshold":    random.choice([5000, 10000, 25000, 50000])
    })

# ── WRITE CSVs ───────────────────────────────────────
def write_csv(filename, rows):
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written: {filename}  ({len(rows)} rows)")

write_csv("data/seed/vendors.csv",   vendors)
write_csv("data/seed/invoices.csv",  invoices)
write_csv("data/seed/payments.csv",  payments)
write_csv("data/seed/approvals.csv", approvals)

print("\nAll seed data generated successfully!")