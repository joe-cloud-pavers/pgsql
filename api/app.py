from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

def get_db_connection():
    return psycopg2.connect(
        host="dbhost.mydomain.com",
        database="mydbname",
        user="user",
        password="password",
        port=5432
    )

@app.route('/users', methods=['GET'])
def get_users():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 4))
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, email, created_at FROM users ORDER BY id LIMIT %s OFFSET %s", (per_page, offset))
    rows = cur.fetchall()

    users = [{"id": r[0], "name": r[1], "email": r[2], "created_at": r[3]} for r in rows]
    
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        "page": page,
        "per_page": per_page,
        "total": total,
        "data": users
    })

@app.route('/users', methods=['POST'])
def create_user():
    data = request.json
    name = data.get('name')
    email = data.get('email')

    if not name or not email:
        return jsonify({"error": "Name and email are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id", (name, email))
        user_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"id": user_id, "name": name, "email": email}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 400
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)


"""
project/
│
├── app.py               # Main Flask app
├── load_data.py         # Script to load sample data
├── requirements.txt     # Dependencies
└── README.md            # Documentation
"""
