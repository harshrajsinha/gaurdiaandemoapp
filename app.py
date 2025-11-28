import os
import vertica_python
from flask import Flask, jsonify, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Vertica Connection Details
conn_info = {
    'host': os.getenv('VERTICA_HOST'),
    'port': int(os.getenv('VERTICA_PORT', 5433)),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
    'database': os.getenv('VERTICA_DB'),
    'connection_timeout': 5
}

SCHEMA = os.getenv('VERTICA_SCHEMA', 'public')

def get_connection():
    return vertica_python.connect(**conn_info)

# Table Schemas (Table Name -> {PK, Columns})
TABLES = {
    'dim_agent': {'pk': 'agent_id', 'columns': ['agent_id', 'agent_name', 'channel', 'region', 'status']},
    'dim_customer': {'pk': 'customer_id', 'columns': ['customer_id', 'customer_name', 'date_of_birth', 'gender', 'marital_status', 'segment', 'occupation', 'city', 'state', 'country']},
    'dim_product': {'pk': 'product_id', 'columns': ['product_id', 'product_name', 'product_type', 'coverage_type', 'term_years']},
    'fact_billing': {'pk': 'bill_id', 'columns': ['bill_id', 'policy_id', 'bill_date', 'bill_amount', 'paid_flag', 'paid_date']},
    'fact_call_center_interaction': {'pk': 'interaction_id', 'columns': ['interaction_id', 'customer_id', 'policy_id', 'interaction_date', 'channel', 'reason_code', 'resolution_status', 'handle_time_seconds']},
    'fact_claim': {'pk': 'claim_id', 'columns': ['claim_id', 'policy_id', 'claim_number', 'claim_type', 'claim_status', 'claim_reported_date', 'claim_closed_date', 'claimed_amount', 'approved_amount']},
    'fact_claim_payment': {'pk': 'payment_id', 'columns': ['payment_id', 'claim_id', 'payment_date', 'payment_amount', 'payment_method']},
    'fact_commission': {'pk': 'comm_id', 'columns': ['comm_id', 'policy_id', 'agent_id', 'commission_amount', 'commission_date']},
    'fact_policy': {'pk': 'policy_id', 'columns': ['policy_id', 'customer_id', 'agent_id', 'product_id', 'policy_number', 'issue_date', 'status', 'sum_assured', 'annual_premium']},
    'fact_underwriting': {'pk': 'uw_id', 'columns': ['uw_id', 'policy_id', 'uw_decision', 'uw_score', 'risk_class', 'decision_date']}
}

@app.route('/')
def index():
    return jsonify({"message": "Vertica Dummy App Running", "tables": list(TABLES.keys())})

def execute_query(query, params=None, fetch=False):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params or {})
            if fetch:
                return cur.fetchall()
            conn.commit()
            return None
    except Exception as e:
        print(f"Error executing query: {e}")
        raise e

# Generate Routes for each table
def create_routes(table_name, schema):
    pk = schema['pk']
    columns = schema['columns']

    @app.route(f'/api/{table_name}', methods=['GET'], endpoint=f'get_{table_name}')
    def get_all():
        try:
            limit = request.args.get('limit', 100)
            query = f"SELECT {', '.join(columns)} FROM {SCHEMA}.{table_name} LIMIT :limit"
            rows = execute_query(query, {'limit': limit}, fetch=True)
            # Convert to list of dicts
            results = [dict(zip(columns, row)) for row in rows]
            return jsonify(results)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route(f'/api/{table_name}', methods=['POST'], endpoint=f'create_{table_name}')
    def create():
        try:
            data = request.json
            # Filter data to only include valid columns
            valid_data = {k: v for k, v in data.items() if k in columns}
            if not valid_data:
                return jsonify({"error": "No valid columns provided"}), 400
            
            cols = ', '.join(valid_data.keys())
            placeholders = ', '.join([f":{k}" for k in valid_data.keys()])
            query = f"INSERT INTO {SCHEMA}.{table_name} ({cols}) VALUES ({placeholders})"
            
            execute_query(query, valid_data)
            return jsonify({"message": "Record created successfully"}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route(f'/api/{table_name}/<id>', methods=['PUT'], endpoint=f'update_{table_name}')
    def update(id):
        try:
            data = request.json
            valid_data = {k: v for k, v in data.items() if k in columns and k != pk}
            if not valid_data:
                return jsonify({"error": "No valid columns provided for update"}), 400

            set_clause = ', '.join([f"{k} = :{k}" for k in valid_data.keys()])
            query = f"UPDATE {SCHEMA}.{table_name} SET {set_clause} WHERE {pk} = :id"
            valid_data['id'] = id
            
            execute_query(query, valid_data)
            return jsonify({"message": "Record updated successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route(f'/api/{table_name}/<id>', methods=['DELETE'], endpoint=f'delete_{table_name}')
    def delete(id):
        try:
            query = f"DELETE FROM {SCHEMA}.{table_name} WHERE {pk} = :id"
            execute_query(query, {'id': id})
            return jsonify({"message": "Record deleted successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

for table, schema in TABLES.items():
    create_routes(table, schema)

# --- Business Logic APIs ---

@app.route('/api/business/customer/<customer_id>/summary', methods=['GET'])
def get_customer_summary(customer_id):
    try:
        # 1. Customer Details
        customer_query = f"SELECT * FROM {SCHEMA}.dim_customer WHERE customer_id = :cid"
        customer = execute_query(customer_query, {'cid': customer_id}, fetch=True)
        if not customer:
            return jsonify({"error": "Customer not found"}), 404
        
        # 2. Policy Summary
        policy_query = f"""
            SELECT COUNT(*) as policy_count, SUM(sum_assured) as total_coverage, SUM(annual_premium) as total_premium 
            FROM {SCHEMA}.fact_policy 
            WHERE customer_id = :cid AND status = 'Active'
        """
        policy_stats = execute_query(policy_query, {'cid': customer_id}, fetch=True)

        # 3. Claims Summary
        claim_query = f"""
            SELECT COUNT(*) as claim_count, SUM(claimed_amount) as total_claimed, SUM(approved_amount) as total_approved
            FROM {SCHEMA}.fact_claim fc
            JOIN {SCHEMA}.fact_policy fp ON fc.policy_id = fp.policy_id
            WHERE fp.customer_id = :cid
        """
        claim_stats = execute_query(claim_query, {'cid': customer_id}, fetch=True)

        # 4. Recent Interactions
        interaction_query = f"""
            SELECT interaction_date, channel, reason_code, resolution_status
            FROM {SCHEMA}.fact_call_center_interaction
            WHERE customer_id = :cid
            ORDER BY interaction_date DESC
            LIMIT 5
        """
        interactions = execute_query(interaction_query, {'cid': customer_id}, fetch=True)

        response = {
            "customer": dict(zip(TABLES['dim_customer']['columns'], customer[0])),
            "policies": {
                "active_count": policy_stats[0][0],
                "total_coverage": float(policy_stats[0][1] or 0),
                "total_premium": float(policy_stats[0][2] or 0)
            },
            "claims": {
                "count": claim_stats[0][0],
                "total_claimed": float(claim_stats[0][1] or 0),
                "total_approved": float(claim_stats[0][2] or 0)
            },
            "recent_interactions": [
                {"date": row[0], "channel": row[1], "reason": row[2], "status": row[3]} 
                for row in interactions
            ]
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/business/agent/performance', methods=['GET'])
def get_agent_performance():
    try:
        query = f"""
            SELECT 
                da.agent_name,
                da.region,
                COUNT(fp.policy_id) as policies_sold,
                SUM(fp.annual_premium) as total_premium,
                SUM(fc.commission_amount) as total_commission
            FROM {SCHEMA}.dim_agent da
            LEFT JOIN {SCHEMA}.fact_policy fp ON da.agent_id = fp.agent_id
            LEFT JOIN {SCHEMA}.fact_commission fc ON fp.policy_id = fc.policy_id
            GROUP BY da.agent_id, da.agent_name, da.region
            ORDER BY total_premium DESC
            LIMIT 20
        """
        rows = execute_query(query, fetch=True)
        results = [
            {
                "agent": row[0],
                "region": row[1],
                "policies_sold": row[2],
                "total_premium": float(row[3] or 0),
                "total_commission": float(row[4] or 0)
            }
            for row in rows
        ]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/business/reports/claims-by-product', methods=['GET'])
def get_claims_by_product():
    try:
        query = f"""
            SELECT 
                dp.product_name,
                dp.product_type,
                COUNT(DISTINCT fp.policy_id) as total_policies,
                SUM(fp.annual_premium) as total_collected_premium,
                COUNT(DISTINCT fc.claim_id) as total_claims,
                SUM(fc.approved_amount) as total_payout
            FROM {SCHEMA}.dim_product dp
            LEFT JOIN {SCHEMA}.fact_policy fp ON dp.product_id = fp.product_id
            LEFT JOIN {SCHEMA}.fact_claim fc ON fp.policy_id = fc.policy_id
            GROUP BY dp.product_id, dp.product_name, dp.product_type
        """
        rows = execute_query(query, fetch=True)
        results = [
            {
                "product": row[0],
                "type": row[1],
                "policies": row[2],
                "premiums": float(row[3] or 0),
                "claims_count": row[4],
                "payouts": float(row[5] or 0),
                "loss_ratio": (float(row[5] or 0) / float(row[3] or 1)) * 100 if row[3] else 0
            }
            for row in rows
        ]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/business/policies/expiring', methods=['GET'])
def get_expiring_policies():
    try:
        # Assuming Vertica syntax for date addition. 
        # Note: This query assumes standard SQL date functions. Vertica might use TIMESTAMPADD.
        # We'll use a generic approach compatible with most or try Vertica specific if known.
        # Vertica: "issue_date + (term_years * 365)" is a rough approx or use ADD_MONTHS
        
        query = f"""
            SELECT 
                fp.policy_number,
                dc.customer_name,
                da.agent_name,
                fp.issue_date,
                dp.term_years,
                ADD_MONTHS(fp.issue_date, dp.term_years * 12) as expiry_date
            FROM {SCHEMA}.fact_policy fp
            JOIN {SCHEMA}.dim_customer dc ON fp.customer_id = dc.customer_id
            JOIN {SCHEMA}.dim_agent da ON fp.agent_id = da.agent_id
            JOIN {SCHEMA}.dim_product dp ON fp.product_id = dp.product_id
            WHERE ADD_MONTHS(fp.issue_date, dp.term_years * 12) BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
        """
        rows = execute_query(query, fetch=True)
        results = [
            {
                "policy_number": row[0],
                "customer": row[1],
                "agent": row[2],
                "issue_date": str(row[3]),
                "term_years": row[4],
                "expiry_date": str(row[5])
            }
            for row in rows
        ]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
