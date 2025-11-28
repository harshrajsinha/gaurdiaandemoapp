# Guardian REST API

This is a Flask-based REST API application that interacts with a Vertica database. It provides CRUD operations for various insurance-related tables and specialized business logic endpoints for analytics.

## Features

-   **CRUD Operations**: Full Create, Read, Update, Delete support for 10+ dimension and fact tables.
-   **Business Analytics**: Specialized endpoints for Customer 360, Agent Performance, and Risk Analysis.
-   **Schema Support**: Configurable database schema via environment variables.

## Prerequisites

-   Python 3.8+
-   Vertica Database access

## Installation

1.  **Clone the repository** (if applicable) or navigate to the project directory.

2.  **Create and activate a virtual environment** (recommended):
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Linux/Mac
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  Create a `.env` file in the root directory (you can copy `.env.example`):
    ```bash
    cp .env.example .env
    ```

2.  Update `.env` with your Vertica credentials:
    ```ini
    VERTICA_HOST=your_host
    VERTICA_PORT=5433
    VERTICA_USER=your_username
    VERTICA_PASSWORD=your_password
    VERTICA_DB=your_database
    VERTICA_SCHEMA=your_schema_name  # Defaults to 'public' if omitted
    ```

## Running the Application

Start the Flask development server:

```bash
python app.py
```

The application will run on `http://localhost:5000`.

## API Documentation

### Standard CRUD Endpoints

Available for tables: `dim_agent`, `dim_customer`, `dim_product`, `fact_billing`, `fact_call_center_interaction`, `fact_claim`, `fact_claim_payment`, `fact_commission`, `fact_policy`, `fact_underwriting`.

-   `GET /api/<table_name>?limit=100` - List records
-   `GET /api/<table_name>/<id>` - Get record details
-   `POST /api/<table_name>` - Create record
-   `PUT /api/<table_name>/<id>` - Update record
-   `DELETE /api/<table_name>/<id>` - Delete record

### Entity APIs (Friendly Aliases)

-   **Customers**
    -   `GET /api/customers` - List customers
    -   `GET /api/customers/<id>` - Customer details
-   **Products**
    -   `GET /api/products` - List products
    -   `GET /api/products/<id>` - Product details
-   **Agents**
    -   `GET /api/agents` - List agents
    -   `GET /api/agents/<id>` - Agent details

### Business Logic Endpoints

-   **Customer 360 View**
    -   `GET /api/business/customer/<customer_id>/summary`
    -   *Aggregated view of customer details, policies, claims, and interactions.*

-   **Agent Performance Leaderboard**
    -   `GET /api/business/agent/performance`
    -   *Top 20 agents by total premium collected.*

-   **Claims Analysis by Product**
    -   `GET /api/business/reports/claims-by-product`
    -   *Profitability and loss ratio analysis per product.*

-   **Expiring Policies**
    -   `GET /api/business/policies/expiring`
    -   *List of policies expiring within the next 30 days.*
