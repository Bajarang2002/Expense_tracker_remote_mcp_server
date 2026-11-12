
import os
import json
import asyncio
from pathlib import Path
from datetime import date
from decimal import Decimal
from contextlib import asynccontextmanager

import asyncpg
from dotenv import load_dotenv
from fastmcp import FastMCP, Context


# --------------------------------------------------
# 1. ENVIRONMENT CONFIGURATION
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

# Load .env from the same folder as this file
load_dotenv(BASE_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "ssl": "require",
}

CATEGORIES_PATH = BASE_DIR / "categories.json"


# Check required database settings
required_settings = ["database", "user", "password"]

missing_settings = [
    key for key in required_settings
    if not DB_CONFIG.get(key)
]

if missing_settings:
    raise RuntimeError(
        f"Missing database settings in .env: {missing_settings}"
    )


# --------------------------------------------------
# 2. DATABASE INITIALIZATION
# --------------------------------------------------

async def init_db(pool: asyncpg.Pool):
    """Create the expenses table if it does not exist."""

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id BIGSERIAL PRIMARY KEY,
                date DATE NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                category VARCHAR(100) NOT NULL,
                subcategory VARCHAR(100) DEFAULT '',
                note TEXT DEFAULT ''
            )
        """)

    print("PostgreSQL database initialized successfully!")


# --------------------------------------------------
# 3. MCP SERVER LIFESPAN
# --------------------------------------------------

@asynccontextmanager
async def app_lifespan(server):
    """
    Create the connection pool at startup.
    Close the pool during server shutdown.
    """

    pool = await asyncpg.create_pool(
        **DB_CONFIG,
        min_size=1,
        max_size=10,
        command_timeout=30
    )

    try:
        await init_db(pool)

        print("Expense Tracker MCP Server started!")

        yield {"pool": pool}

    finally:
        await pool.close()
        print("PostgreSQL connection pool closed.")


# Initialize MCP server
mcp = FastMCP(
    name="ExpenseTracker",
    lifespan=app_lifespan
)


# --------------------------------------------------
# 4. HELPER FUNCTIONS
# --------------------------------------------------

def get_pool(ctx: Context) -> asyncpg.Pool:
    """Get the shared PostgreSQL pool from MCP lifespan."""

    return ctx.lifespan_context["pool"]


def parse_date(value: str) -> date:
    """Convert YYYY-MM-DD string into a Python date."""

    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError(
            "Invalid date. Use YYYY-MM-DD format, e.g. 2026-11-11."
        )


def validate_date_range(start_date: str, end_date: str):
    """Validate date inputs and their order."""

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        raise ValueError("start_date cannot be after end_date.")

    return start, end


# --------------------------------------------------
# 5. ADD EXPENSE
# --------------------------------------------------

@mcp.tool()
async def add_expense(
    date: str,
    amount: float,
    category: str,
    subcategory: str = "",
    note: str = "",
    ctx: Context = None
):
    """Add a new expense entry to PostgreSQL."""

    if amount <= 0:
        raise ValueError("Amount must be greater than zero.")

    expense_date = parse_date(date)
    pool = get_pool(ctx)

    async with pool.acquire() as conn:
        expense_id = await conn.fetchval(
            """
            INSERT INTO expenses
                (date, amount, category, subcategory, note)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            expense_date,
            Decimal(str(amount)),
            category,
            subcategory,
            note
        )

    return {
        "status": "success",
        "message": "Expense added successfully",
        "id": expense_id
    }


# --------------------------------------------------
# 6. LIST EXPENSES
# --------------------------------------------------

@mcp.tool()
async def list_expenses(
    start_date: str,
    end_date: str,
    ctx: Context
):
    """List expenses within an inclusive date range."""

    start, end = validate_date_range(start_date, end_date)
    pool = get_pool(ctx)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                date,
                amount,
                category,
                subcategory,
                note
            FROM expenses
            WHERE date BETWEEN $1 AND $2
            ORDER BY date ASC, id ASC
            """,
            start,
            end
        )

    return [
        {
            "id": row["id"],
            "date": row["date"].isoformat(),
            "amount": float(row["amount"]),
            "category": row["category"],
            "subcategory": row["subcategory"],
            "note": row["note"]
        }
        for row in rows
    ]


# --------------------------------------------------
# 7. SUMMARIZE EXPENSES
# --------------------------------------------------

@mcp.tool()
async def summarize(
    start_date: str,
    end_date: str,
    category: str = None,
    ctx: Context = None
):
    """Summarize expenses by category within a date range."""

    start, end = validate_date_range(start_date, end_date)
    pool = get_pool(ctx)

    async with pool.acquire() as conn:

        if category:
            rows = await conn.fetch(
                """
                SELECT
                    category,
                    SUM(amount) AS total_amount
                FROM expenses
                WHERE date BETWEEN $1 AND $2
                  AND category = $3
                GROUP BY category
                ORDER BY category ASC
                """,
                start,
                end,
                category
            )

        else:
            rows = await conn.fetch(
                """
                SELECT
                    category,
                    SUM(amount) AS total_amount
                FROM expenses
                WHERE date BETWEEN $1 AND $2
                GROUP BY category
                ORDER BY category ASC
                """,
                start,
                end
            )

    return [
        {
            "category": row["category"],
            "total_amount": float(row["total_amount"])
        }
        for row in rows
    ]


# --------------------------------------------------
# 8. DELETE EXPENSE
# --------------------------------------------------

@mcp.tool()
async def delete_expense(
    expense_id: int,
    ctx: Context
):
    """Delete an expense using its ID."""

    pool = get_pool(ctx)

    async with pool.acquire() as conn:
        deleted_id = await conn.fetchval(
            """
            DELETE FROM expenses
            WHERE id = $1
            RETURNING id
            """,
            expense_id
        )

    if deleted_id is None:
        return {
            "status": "error",
            "message": f"Expense with ID {expense_id} not found"
        }

    return {
        "status": "success",
        "message": "Expense deleted successfully",
        "deleted_id": deleted_id
    }


# --------------------------------------------------
# 9. UPDATE EXPENSE
# --------------------------------------------------



@mcp.tool()
async def update_expense(
    ctx: Context,
    expense_id: int,
    date: str | None = None,
    amount: float | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    note: str | None = None
):
    """Update selected fields of an existing expense."""

    updates = {}

    # Validate and collect fields
    if date is not None and date.strip():
        updates["date"] = parse_date(date)

    if amount is not None:
        if amount <= 0:
            raise ValueError("Amount must be greater than zero.")

        updates["amount"] = Decimal(str(amount))

    if category is not None and category.strip():
        updates["category"] = category.strip()

    if subcategory is not None and subcategory.strip():
        updates["subcategory"] = subcategory.strip()

    if note is not None and note.strip():
        updates["note"] = note.strip()

    if not updates:
        return {
            "status": "error",
            "message": "Provide at least one field to update."
        }

    pool = get_pool(ctx)

    set_clauses = []
    values = []

    for column, value in updates.items():
        values.append(value)
        set_clauses.append(f"{column} = ${len(values)}")

    values.append(expense_id)
    id_position = len(values)

    query = f"""
        UPDATE expenses
        SET {", ".join(set_clauses)}
        WHERE id = ${id_position}
        RETURNING id
    """

    async with pool.acquire() as conn:
        updated_id = await conn.fetchval(query, *values)

    if updated_id is None:
        return {
            "status": "error",
            "message": f"Expense with ID {expense_id} not found"
        }

    return {
        "status": "success",
        "message": "Expense updated successfully",
        "expense_id": updated_id,
        "updated_fields": list(updates.keys())
    }



# --------------------------------------------------
# 10. EXPENSE CATEGORIES RESOURCE
# --------------------------------------------------

@mcp.resource(
    "expense://categories",
    mime_type="application/json"
)
async def categories():
    """Read expense categories from categories.json."""

    if not CATEGORIES_PATH.exists():
        raise FileNotFoundError(
            f"Categories file not found: {CATEGORIES_PATH}"
        )

    content = await asyncio.to_thread(
        CATEGORIES_PATH.read_text,
        encoding="utf-8"
    )

    # Check that the file contains valid JSON
    json.loads(content)

    return content


# --------------------------------------------------
# 11. RUN MCP SERVER
# --------------------------------------------------

if __name__ == "__main__":
    print("Starting Async Expense Tracker MCP Server...")
    mcp.run(transport= "http",host = "0.0.0.0",port= 8000)
