import tkinter as tk
from tkinter import scrolledtext, messagebox
import sqlite3
import time
import re
import json
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


# -----------------------------------------------------------
# SAMPLE DATABASE CREATION
# -----------------------------------------------------------
def create_sample_db(conn):
    cur = conn.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS order_items;

        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT
        );

        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL
        );

        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            total REAL
        );

        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL
        );
    """)

    # Insert sample data
    customers = [(i, f'Cust_{i}', f'City_{i%5}') for i in range(1, 51)]
    cur.executemany("INSERT INTO customers VALUES (?, ?, ?)", customers)

    products = [(i, f'Prod_{i}', (i % 20) + 5) for i in range(1, 101)]
    cur.executemany("INSERT INTO products VALUES (?, ?, ?)", products)

    orders = [(i, (i % 50) + 1, "2025-11-01", 0) for i in range(1, 201)]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", orders)

    item_data = []
    import random
    oid = 1
    for o in range(1, 201):
        for _ in range(random.randint(1, 5)):
            pid = random.randint(1, 100)
            qty = random.randint(1, 5)
            price = (pid % 20) + 5
            item_data.append((oid, o, pid, qty, price))
            oid += 1

    cur.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?)", item_data)

    cur.execute("""
        UPDATE orders
        SET total = (SELECT SUM(quantity * unit_price)
                     FROM order_items oi WHERE oi.order_id = orders.order_id)
    """)

    conn.commit()


# -----------------------------------------------------------
# ANALYSIS HELPERS
# -----------------------------------------------------------
def count_joins_and_subqueries(sql):
    return (
        len(re.findall(r"\bJOIN\b", sql.upper())),
        len(re.findall(r"\(\s*SELECT", sql.upper()))
    )


def extract_tables(sql):
    tables = set()
    tables.update(re.findall(r"FROM\s+([^\s;]+)", sql, re.IGNORECASE))
    tables.update(re.findall(r"JOIN\s+([^\s;]+)", sql, re.IGNORECASE))
    return list(tables)


def explain_query(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute("EXPLAIN QUERY PLAN " + sql)
        return cur.fetchall()
    except Exception as e:
        return [("EXPLAIN FAILED", str(e))]


def time_query(conn, sql):
    cur = conn.cursor()
    start = time.time()
    try:
        cur.execute(sql)
        cur.fetchall()
        return time.time() - start, None
    except Exception as e:
        return None, str(e)


def table_scan_time(conn, table):
    cur = conn.cursor()
    try:
        start = time.time()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        cur.fetchall()
        return time.time() - start
    except:
        return 0


def join_cost(conn, t1, t2):
    cur = conn.cursor()
    try:
        start = time.time()
        cur.execute(f"SELECT COUNT(*) FROM {t1}, {t2} LIMIT 2000")
        cur.fetchall()
        return time.time() - start
    except:
        return 0


# -----------------------------------------------------------
# PDF GENERATION
# -----------------------------------------------------------
def generate_pdf(report_text, timeline, explain_rows):

    c = canvas.Canvas("analysis_report.pdf", pagesize=letter)
    width, height = letter

    y = height - 50
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "SQL Query Analysis Report")
    y -= 40

    c.setFont("Helvetica", 9)

    # Write report text
    for line in report_text.split("\n"):
        if y < 40:
            c.showPage()
            c.setFont("Helvetica", 9)
            y = height - 50
        c.drawString(50, y, line)
        y -= 15

    c.showPage()
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, height - 50, "Execution Timeline (Sorted)")
    y = height - 80
    c.setFont("Helvetica", 9)

    for t in timeline:
        line = f"{t['type']} | {t['name']} | {t['duration']:.6f}s"
        if y < 40:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 9)
        c.drawString(50, y, line)
        y -= 15

    c.showPage()
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, height - 50, "EXPLAIN QUERY PLAN")
    y = height - 80
    c.setFont("Helvetica", 9)

    for r in explain_rows:
        if y < 40:
            c.showPage()
            y = height - 50
        c.drawString(50, y, str(r))
        y -= 15

    c.save()


# -----------------------------------------------------------
# REPORT TEXT BUILDER
# -----------------------------------------------------------
def generate_report(sql, joins, subs, tables, exec_time, explain_rows, timeline_sorted, bottlenecks):
    report = []
    report.append("=== SQL ANALYSIS REPORT ===")
    report.append(f"Join count: {joins}")
    report.append(f"Subquery count: {subs}")
    report.append(f"Tables: {tables}")
    report.append(f"Full query duration (s): {exec_time:.6f}")

    report.append("\nTop timeline steps (sorted by measured duration):")
    for step in timeline_sorted[:10]:
        report.append(
            f" - {step['type']:10s} | {step['name'][:60]:60s} | {step['duration']:.6f}s"
        )

    report.append("\nBottlenecks:")
    for b in bottlenecks:
        report.append(f" * {b['type']} - {b['name']} : {b['duration']:.6f}s")

    report.append("\nEXPLAIN QUERY PLAN (raw):")
    for r in explain_rows:
        report.append("   " + str(r))

    return "\n".join(report)


# -----------------------------------------------------------
# MAIN GUI LOGIC
# -----------------------------------------------------------
def analyze_sql():
    sql = sql_input.get("1.0", tk.END).strip()
    if not sql:
        messagebox.showerror("Error", "Please enter SQL query.")
        return

    joins, subs = count_joins_and_subqueries(sql)
    tables = extract_tables(sql)
    explain_rows = explain_query(conn, sql)
    exec_time, error = time_query(conn, sql)

    if error:
        messagebox.showerror("SQL Error", error)
        return

    timeline = []

    for t in tables:
        timeline.append({
            "name": f"Table scan: {t}",
            "type": "table_scan",
            "duration": table_scan_time(conn, t)
        })

    for i in range(len(tables)):
        for j in range(i + 1, len(tables)):
            t1, t2 = tables[i], tables[j]
            timeline.append({
                "name": f"Join {t1} ⨝ {t2}",
                "type": "join",
                "duration": join_cost(conn, t1, t2)
            })

    timeline.append({
        "name": "Full query execution",
        "type": "query",
        "duration": exec_time
    })

    timeline_sorted = sorted(timeline, key=lambda x: x["duration"], reverse=True)
    bottlenecks = timeline_sorted[:5]

    report_text = generate_report(sql, joins, subs, tables, exec_time, explain_rows, timeline_sorted, bottlenecks)

    # Display in GUI
    output_box.delete("1.0", tk.END)
    output_box.insert(tk.END, report_text)

    # Save TXT (UTF-8 FIXED)
    with open("analysis_report.txt", "w", encoding="utf-8") as f:
        f.write(report_text)

    # Save JSON (UTF-8 FIXED)
    with open("analysis_report.json", "w", encoding="utf-8") as f:
        json.dump({
            "sql": sql,
            "join_count": joins,
            "subquery_count": subs,
            "tables": tables,
            "execution_time": exec_time,
            "timeline": timeline_sorted,
            "bottlenecks": bottlenecks,
            "explain": [str(r) for r in explain_rows]
        }, f, indent=4, ensure_ascii=False)

    # Save PDF
    generate_pdf(report_text, timeline_sorted, explain_rows)

    messagebox.showinfo("Success", "Reports Generated:\n• analysis_report.txt\n• analysis_report.json\n• analysis_report.pdf")


# -----------------------------------------------------------
# BUILD GUI INTERFACE
# -----------------------------------------------------------
root = tk.Tk()
root.title("Dynamic SQL Query Analyzer (PDF + UTF-8 Fixed)")
root.geometry("1000x650")

tk.Label(root, text="Dynamic SQL Query Analyzer", font=("Arial", 20, "bold")).pack(pady=10)

tk.Label(root, text="Enter SQL Query Below:").pack()
sql_input = scrolledtext.ScrolledText(root, width=120, height=8)
sql_input.pack(pady=5)

tk.Button(root, text="Analyze Query",
          font=("Arial", 12, "bold"),
          bg="green", fg="white",
          command=analyze_sql).pack(pady=10)

output_box = scrolledtext.ScrolledText(root, width=120, height=25)
output_box.pack(pady=10)

# Load sample DB
conn = sqlite3.connect(":memory:")
create_sample_db(conn)

root.mainloop()
