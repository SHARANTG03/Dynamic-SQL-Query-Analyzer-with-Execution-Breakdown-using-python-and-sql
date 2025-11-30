#!/usr/bin/env python3
"""
sql_analyzer.py

Dynamic SQL Query Analyzer with Execution Breakdown.

Features:
- Parse SQL query for join count and subquery count (prefer sqlparse, fallback to heuristics).
- Use SQLite to create sample data or connect to a database file (optional).
- Use EXPLAIN QUERY PLAN to get plan steps.
- Run timings for whole query, per-table operations, pairwise joins, and subqueries (if possible).
- Produce an execution timeline and identify bottlenecks.

Usage:
    python sql_analyzer.py
    (edit EXAMPLE_QUERY or pass queries to functions in code)
"""

import sqlite3
import time
import re
import json
import sys
from collections import defaultdict, namedtuple

# Try to use sqlparse for better parsing
try:
    import sqlparse
    HAS_SQLPARSE = True
except Exception:
    HAS_SQLPARSE = False

Step = namedtuple("Step", ["name", "type", "duration_s", "detail"])

# ----------------------
# Utility: Database Setup
# ----------------------
def create_sample_db(conn):
    """Create sample tables and populate with synthetic data for simulation."""
    cur = conn.cursor()
    cur.executescript("""
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
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
        total REAL,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE order_items (
        order_item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        FOREIGN KEY(order_id) REFERENCES orders(order_id),
        FOREIGN KEY(product_id) REFERENCES products(product_id)
    );
    """)
    # Insert moderate amount of data
    # customers
    customers = [(i, f'Cust_{i}', 'City_'+str((i%10)+1)) for i in range(1, 201)]
    cur.executemany("INSERT INTO customers(customer_id, name, city) VALUES (?, ?, ?);", customers)
    # products
    products = [(i, f'Prod_{i}', round(5.0 + (i % 20) * 1.5, 2)) for i in range(1, 501)]
    cur.executemany("INSERT INTO products(product_id, name, price) VALUES (?, ?, ?);", products)
    # orders
    orders = []
    oid = 1
    for cust in range(1, 201):
        # each customer has 1..5 orders
        for j in range((cust % 5) + 1):
            orders.append((oid, cust, f'2025-11-{(j%28)+1:02d}', 0.0))
            oid += 1
    cur.executemany("INSERT INTO orders(order_id, customer_id, order_date, total) VALUES (?, ?, ?, ?);", orders)
    # order_items
    order_items = []
    oitem_id = 1
    import random
    random.seed(42)
    for order in range(1, oid):
        # each order has 1..6 items
        for k in range(1, (order % 6) + 1):
            pid = random.randint(1, 500)
            qty = random.randint(1, 5)
            price = 5.0 + (pid % 20) * 1.5
            order_items.append((oitem_id, order, pid, qty, price))
            oitem_id += 1
    cur.executemany("INSERT INTO order_items(order_item_id, order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?, ?);", order_items)

    # update orders.total
    cur.execute("""
        UPDATE orders
        SET total = (
            SELECT SUM(quantity * unit_price) FROM order_items WHERE order_items.order_id = orders.order_id
        )
    """)
    conn.commit()
    print("Sample DB created: customers(200), products(500), orders(~1000), order_items(~4000)")

# ----------------------
# SQL Parsing (joins, subqueries, tables)
# ----------------------
def count_joins_and_subqueries(sql: str):
    """Return (join_count, subquery_count). Uses sqlparse if available, else heuristics."""
    sql_up = sql.upper()
    # heuristic: count occurrences of ' JOIN ' ignoring inside strings
    join_count = len(re.findall(r'\bJOIN\b', sql_up))
    # subquery heuristic: count 'SELECT' occurrences besides the first top-level one
    # More robust approach: count occurrences of '(' followed by SELECT
    subquery_count = len(re.findall(r'\(\s*SELECT\b', sql, flags=re.IGNORECASE))
    return join_count, subquery_count

def extract_table_names(sql: str):
    """
    Attempt to extract table names from FROM and JOIN clauses.
    If sqlparse is available, use tokens; else use regex heuristics.
    Returns a list of table names (may include aliases).
    """
    tables = set()
    if HAS_SQLPARSE:
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            from_seen = False
            for token in statement.tokens:
                ttype = token.ttype
                sval = str(token)
                # naive: look for 'FROM' or 'JOIN' keywords in flattened tokens
            # Use sqlparse utilities (flatten)
        # fallback to regex anyway (sqlparse parsing for full table name extraction is long)
    # regex to find FROM <table> and JOIN <table>
    for match in re.finditer(r'\bFROM\s+([^\s,;]+)', sql, flags=re.IGNORECASE):
        tables.add(clean_table_token(match.group(1)))
    for match in re.finditer(r'\bJOIN\s+([^\s,;]+)', sql, flags=re.IGNORECASE):
        tables.add(clean_table_token(match.group(1)))
    return list(tables)

def clean_table_token(token):
    """Clean table token by removing aliases and punctuation."""
    t = token.strip()
    # remove trailing commas
    t = re.sub(r',$', '', t)
    # if token contains AS or alias, take first part
    t = re.split(r'\s+AS\s+|\s+', t, flags=re.IGNORECASE)[0]
    # remove parentheses
    t = t.strip('()')
    return t

def extract_subqueries(sql: str):
    """Return list of subquery strings found (very simple heuristic using balanced parentheses)."""
    subs = []
    # find '( SELECT ... )' capturing balanced parentheses roughly
    pattern = re.compile(r'\(\s*SELECT', flags=re.IGNORECASE)
    for m in pattern.finditer(sql):
        start = m.start()
        # find matching closing parenthesis naive parse
        depth = 0
        i = start
        while i < len(sql):
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
                if depth == 0:
                    subs.append(sql[start:i+1])
                    break
            i += 1
    return subs

# ----------------------
# Analyzer: EXPLAIN + timing
# ----------------------
def explain_query_plan(conn, sql):
    """Run EXPLAIN QUERY PLAN and return rows."""
    cur = conn.cursor()
    try:
        cur.execute("EXPLAIN QUERY PLAN " + sql)
        rows = cur.fetchall()
        # rows are tuples (id, parent, notused, detail) or text depending on sqlite version
        return rows
    except sqlite3.DatabaseError as e:
        print("EXPLAIN failed:", e)
        return []

def time_query(conn, sql, warmup=True, iterations=1):
    """Time execution of SQL. Returns elapsed seconds and optionally result count."""
    cur = conn.cursor()
    # warmup
    if warmup:
        try:
            cur.execute(sql)
            cur.fetchall()
        except Exception:
            pass
    start = time.perf_counter()
    results = None
    try:
        for _ in range(iterations):
            cur.execute(sql)
            results = cur.fetchall()
    except Exception as e:
        # Could be invalid fragment (we'll capture as zero time and note error)
        return None, str(e)
    elapsed = time.perf_counter() - start
    # duration per iteration
    per_iter = elapsed / max(1, iterations)
    return per_iter, results

def estimate_table_costs(conn, tables):
    """
    Estimate per-table cost by running a simple aggregation on each table.
    Returns dict table -> duration(s)
    """
    costs = {}
    for t in tables:
        # Check table exists
        try:
            sql = f"SELECT COUNT(*) FROM {t}"
            dur, _ = time_query(conn, sql)
            if dur is None:
                costs[t] = None
            else:
                costs[t] = dur
        except Exception as e:
            costs[t] = None
    return costs

def estimate_join_cost(conn, left_table, right_table, join_condition=None):
    """
    Estimate cost of joining two tables by executing a small join with LIMIT.
    join_condition can be provided like 'left.col = right.col' or will guess on common names.
    """
    # Build best-effort join SQL
    base_left = left_table
    base_right = right_table
    # attempt to find common column names (id-based) by querying PRAGMA table_info
    cur = conn.cursor()
    try:
        left_cols = [r[1] for r in cur.execute(f"PRAGMA table_info({base_left})").fetchall()]
        right_cols = [r[1] for r in cur.execute(f"PRAGMA table_info({base_right})").fetchall()]
    except Exception:
        left_cols = []
        right_cols = []
    common = set(left_cols).intersection(set(right_cols))
    if join_condition:
        cond = join_condition
    elif common:
        c = next(iter(common))
        cond = f"{base_left}.{c} = {base_right}.{c}"
    else:
        # fallback - do cross join but limit rows
        cond = None
    if cond:
        sql = f"SELECT COUNT(*) FROM {base_left} JOIN {base_right} ON {cond}"
    else:
        sql = f"SELECT COUNT(*) FROM {base_left}, {base_right} LIMIT 1000"
    dur, _ = time_query(conn, sql)
    return dur

# ----------------------
# High-level analyze function
# ----------------------
def analyze_query(conn, sql):
    """
    Perform full analysis returning:
      - join_count, subquery_count
      - tables involved
      - explain plan
      - measured timings and execution timeline steps
      - bottleneck identification
    """
    report = {}
    sql = sql.strip().rstrip(';')
    join_count, subquery_count = count_joins_and_subqueries(sql)
    report['join_count'] = join_count
    report['subquery_count'] = subquery_count

    tables = extract_table_names(sql)
    report['tables'] = tables

    report['explain'] = explain_query_plan(conn, sql)

    # Measure full query time
    print("Timing full query...")
    full_dur, full_results = time_query(conn, sql, warmup=True, iterations=1)
    report['full_query_duration_s'] = full_dur

    timeline = []

    # Add explain steps to timeline (if present)
    for row in report['explain']:
        # row may be different shapes; convert to str
        timeline.append(Step(name=str(row), type="explain_step", duration_s=0.0, detail=str(row))._asdict())

    # Per-table estimates
    print("Estimating per-table costs...")
    table_costs = estimate_table_costs(conn, tables)
    for t, d in table_costs.items():
        timeline.append(Step(name=f"Table scan: {t}", type="table_scan", duration_s=d or 0.0, detail="COUNT(*) estimate")._asdict())

    # For join pairs, estimate join cost (attempt for each pair)
    join_costs = {}
    if len(tables) >= 2:
        print("Estimating join costs (pairwise)...")
        for i in range(len(tables)):
            for j in range(i+1, len(tables)):
                left = tables[i]
                right = tables[j]
                dur = estimate_join_cost(conn, left, right)
                key = f"{left}<> {right}"
                join_costs[key] = dur
                timeline.append(Step(name=f"Join {left} ⨝ {right}", type="join", duration_s=dur or 0.0, detail="pairwise join COUNT(*)")._asdict())

    # Subqueries: extract and time each
    subqueries = extract_subqueries(sql)
    subq_costs = {}
    if subqueries:
        print("Timing subqueries...")
        for idx, sub in enumerate(subqueries, start=1):
            # Remove surrounding parentheses
            ssql = sub.strip()
            if ssql[0] == '(' and ssql[-1] == ')':
                ssql = ssql[1:-1]
            dur, _ = time_query(conn, ssql)
            subq_costs[f"subquery_{idx}"] = dur
            timeline.append(Step(name=f"Subquery {idx}", type="subquery", duration_s=dur or 0.0, detail=ssql[:200])._asdict())

    # Whole query step
    timeline.append(Step(name="Full query execution", type="query", duration_s=full_dur or 0.0, detail=sql[:400])._asdict())

    # Sort timeline by duration desc for bottleneck identification
    timeline_sorted = sorted(timeline, key=lambda s: (s['duration_s'] if s['duration_s'] is not None else 0.0), reverse=True)
    report['timeline'] = timeline_sorted

    # Identify top bottlenecks (top 3 durations > threshold)
    bottlenecks = []
    for step in timeline_sorted[:5]:
        # choose candidate steps with non-zero duration
        if step['duration_s'] and step['duration_s'] > 0:
            bottlenecks.append(step)
    report['bottlenecks'] = bottlenecks

    # include measured tables/join/subquery costs
    report['table_costs'] = table_costs
    report['join_costs'] = join_costs
    report['subquery_costs'] = subq_costs
    return report

# ----------------------
# Pretty printing + save
# ----------------------
def print_report(report):
    print("\n=== SQL ANALYSIS REPORT ===")
    print(f"Join count: {report.get('join_count')}")
    print(f"Subquery count: {report.get('subquery_count')}")
    print("Tables:", report.get('tables'))
    print(f"Full query duration (s): {report.get('full_query_duration_s')}")
    print("\nTop timeline steps (sorted by measured duration):")
    timeline = report.get('timeline', [])[:10]
    for step in timeline:
        dur = step['duration_s']
        print(f" - {step['type']:10s} | {step['name'][:60]:60s} | {dur:.6f}s")
    print("\nBottlenecks:")
    for b in report.get('bottlenecks', []):
        print(f" * {b['type']} - {b['name'][:80]} : {b['duration_s']:.6f}s")
    print("\nEXPLAIN QUERY PLAN (raw):")
    for r in report.get('explain', []):
        print("  ", r)
    print("\n(Report also saved to sql_analysis_report.json)")

def save_report(report, filename="sql_analysis_report.json"):
    with open(filename, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Saved report to {filename}")

# ----------------------
# Example usage
# ----------------------
if __name__ == "__main__":
    # Connect to in-memory sqlite (or change to a file db)
    conn = sqlite3.connect(":memory:")
    create_sample_db(conn)

    # Example query (you can replace this with your query)
    EXAMPLE_QUERY = """
    SELECT c.customer_id, c.name, o.order_id, SUM(oi.quantity * oi.unit_price) AS order_total
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE p.price > 20
        AND c.city = 'City_1'
        AND o.order_date >= '2025-11-01'
    GROUP BY o.order_id
    HAVING order_total > 50
    ORDER BY order_total DESC
    LIMIT 10;


    """

    print("Analyzing example query (edit EXAMPLE_QUERY in the script to test your own)...\n")
    report = analyze_query(conn, EXAMPLE_QUERY)
    print_report(report)
    save_report(report)
