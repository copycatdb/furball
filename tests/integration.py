#!/usr/bin/env python3
"""Integration tests for furball ODBC driver against SQL Server 2022."""

import pyodbc
import sys

CONN_STR = "Driver={furball};Server=localhost,1433;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes;"


def test_select_one(cur):
    cur.execute("SELECT 1 AS val")
    row = cur.fetchone()
    assert row[0] == 1, f"Expected 1, got {row[0]}"
    print("PASS: SELECT 1")


def test_multi_row(cur):
    cur.execute("SELECT n FROM (VALUES (1),(2),(3)) AS t(n)")
    rows = cur.fetchall()
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
    assert [r[0] for r in rows] == [1, 2, 3]
    print("PASS: Multi-row query")


def test_parameterized(cur):
    cur.execute("SELECT ? + ? AS total", 10, 20)
    row = cur.fetchone()
    assert row[0] == 30, f"Expected 30, got {row[0]}"
    print("PASS: Parameterized query")


def test_insert_rowcount(cur):
    cur.execute("CREATE TABLE #test_rc (id INT, name NVARCHAR(50))")
    cur.execute("INSERT INTO #test_rc VALUES (1, N'alice')")
    assert cur.rowcount == 1, f"Expected rowcount 1, got {cur.rowcount}"
    cur.execute("INSERT INTO #test_rc VALUES (2, N'bob'), (3, N'charlie')")
    assert cur.rowcount == 2, f"Expected rowcount 2, got {cur.rowcount}"
    print("PASS: INSERT + rowcount")


def test_transactions(conn):
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("CREATE TABLE #test_tx (id INT)")
    cur.execute("INSERT INTO #test_tx VALUES (1)")
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM #test_tx")
    assert cur.fetchone()[0] == 1
    cur.execute("INSERT INTO #test_tx VALUES (2)")
    conn.rollback()
    cur.execute("SELECT COUNT(*) FROM #test_tx")
    assert cur.fetchone()[0] == 1, "Rollback should have undone the insert"
    conn.autocommit = True
    print("PASS: Transactions (autocommit off/on, commit, rollback)")


def test_cursor_tables(cur):
    results = cur.tables(tableType="TABLE").fetchall()
    # Just verify the call works and returns column metadata
    assert results is not None
    print(f"PASS: cursor.tables() returned {len(results)} tables")


def test_cursor_columns(cur):
    # Query columns from a known system view
    results = cur.columns(table="spt_monitor", catalog="master", schema="dbo").fetchall()
    assert results is not None
    print(f"PASS: cursor.columns() returned {len(results)} columns")


def main():
    print(f"Connecting with: {CONN_STR}")
    conn = pyodbc.connect(CONN_STR)
    cur = conn.cursor()

    tests = [
        lambda: test_select_one(cur),
        lambda: test_multi_row(cur),
        lambda: test_parameterized(cur),
        lambda: test_insert_rowcount(cur),
        lambda: test_transactions(conn),
        lambda: test_cursor_tables(cur),
        lambda: test_cursor_columns(cur),
    ]

    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"FAIL: {e}")
            failed += 1

    conn.close()

    if failed:
        print(f"\n{failed} test(s) failed")
        sys.exit(1)
    else:
        print("\nAll tests passed!")


if __name__ == "__main__":
    main()
