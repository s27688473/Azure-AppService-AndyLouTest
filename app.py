"""
Flask Multi-Database Demo
支援：Azure SQL Database、Azure PostgreSQL、Cosmos DB (MongoDB API)

路由結構：
  GET  /                          → 首頁 (index.html)
  GET  /azure-sql                 → Azure SQL 頁面 (azure_sql.html)
  GET  /postgres                  → PostgreSQL 頁面 (postgres.html)
  GET  /cosmos-mongo              → Cosmos DB 頁面 (cosmos_mongo.html)

各 DB 的 API（被各自頁面的 JS fetch 呼叫）：
  POST /api/azure-sql/register    → 新增使用者到 Azure SQL
  GET  /api/azure-sql/search      → 從 Azure SQL 查詢暱稱
  POST /api/postgres/register     → 新增使用者到 PostgreSQL
  GET  /api/postgres/search       → 從 PostgreSQL 查詢暱稱
  POST /api/cosmos-mongo/register → 新增使用者到 Cosmos DB
  GET  /api/cosmos-mongo/search   → 從 Cosmos DB 查詢暱稱

資料表 / Collection 結構（name + nickname）：
  Azure SQL / PostgreSQL:
    CREATE TABLE users (
        id       SERIAL / INT IDENTITY PRIMARY KEY,
        name     VARCHAR(100) UNIQUE NOT NULL,
        nickname VARCHAR(100) NOT NULL
    );
  Cosmos DB (MongoDB API):
    { name: "Alice", nickname: "小愛" }
"""

import os
from flask import Flask, render_template, request, jsonify


import pyodbc                       # Azure SQL
import psycopg2                     # Azure PostgreSQL
from pymongo import MongoClient     # Cosmos DB (MongoDB API)


app = Flask(__name__)


# ════════════════════════════════════════════════════════════════════════
#  DB 連線函式
# ════════════════════════════════════════════════════════════════════════

def get_sql_conn():
    """回傳 Azure SQL Database 連線"""
    conn_str = (
        f"DRIVER={os.getenv('AZURE_SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USER')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def get_pg_conn():
    """回傳 Azure PostgreSQL 連線"""
    return psycopg2.connect(os.getenv("AZURE_POSTGRESQL_CONNECTIONSTRING"))


def get_mongo_col():
    """回傳 Cosmos DB (MongoDB API) Collection"""
    client = MongoClient(os.getenv("AZURE_COSMOS_CONNECTIONSTRING"))
    return client[os.getenv("COSMOS_MONGO_DATABASE")]["users"]

# ════════════════════════════════════════════════════════════════════════
#  頁面路由（render html）
# ════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/azure-sql")
def page_azure_sql():
    return render_template("azure_sql.html")


@app.route("/postgres")
def page_postgres():
    return render_template("postgres.html")


@app.route("/cosmos-mongo")
def page_cosmos_mongo():
    return render_template("cosmos_mongo.html")


# ════════════════════════════════════════════════════════════════════════
#  API：Azure SQL Database
# ════════════════════════════════════════════════════════════════════════

@app.route("/api/azure-sql/register", methods=["POST"])
def sql_register():
    """新增 name + nickname 到 Azure SQL"""
    data     = request.get_json(force=True)
    name     = (data.get("name", "") or "").strip()
    nickname = (data.get("nickname", "") or "").strip()

    if not name or not nickname:
        return jsonify({"message": "姓名與暱稱不能為空"}), 400

    try:
        conn   = get_sql_conn()
        cursor = conn.cursor()
        # 自動建立資料表（若不存在）
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
            CREATE TABLE users (
                id       INT IDENTITY(1,1) PRIMARY KEY,
                name     NVARCHAR(100) NOT NULL UNIQUE,
                nickname NVARCHAR(100) NOT NULL
            );
        """)
        conn.commit()
        # 若姓名已存在則更新暱稱，否則新增
        cursor.execute("SELECT 1 FROM users WHERE name = ?", (name,))
        if cursor.fetchone():
            cursor.execute("UPDATE users SET nickname = ? WHERE name = ?", (nickname, name))
            msg = f"已更新 {name} 的暱稱為 {nickname}。"
        else:
            cursor.execute("INSERT INTO users (name, nickname) VALUES (?, ?)", (name, nickname))
            msg = f"成功註冊 {name}，暱稱：{nickname}。"
        conn.commit()
        conn.close()
        return jsonify({"message": msg}), 200
    except Exception as e:
        app.logger.error(f"[Azure SQL] register error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500

@app.route("/api/azure-sql/search", methods=["GET"])
def sql_search():
    """從 Azure SQL 查詢 name 對應的 nickname"""
    name = (request.args.get("name", "") or "").strip()
    if not name:
        return jsonify({"message": "請提供姓名參數"}), 400

    try:
        conn   = get_sql_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT nickname FROM users WHERE name = ?", (name,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return jsonify({"nickname": row[0]}), 200
        return jsonify({"message": f"找不到名為「{name}」的使用者。"}), 404
    except Exception as e:
        app.logger.error(f"[Azure SQL] search error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500


# ════════════════════════════════════════════════════════════════════════
#  API：Azure PostgreSQL
# ════════════════════════════════════════════════════════════════════════

@app.route("/api/postgres/register", methods=["POST"])
def pg_register():
    """新增 name + nickname 到 Azure PostgreSQL"""
    data     = request.get_json(force=True)
    name     = (data.get("name", "") or "").strip()
    nickname = (data.get("nickname", "") or "").strip()

    if not name or not nickname:
        return jsonify({"message": "姓名與暱稱不能為空"}), 400

    try:
        conn = get_pg_conn()
        cur  = conn.cursor()
        # 自動建立資料表（若不存在）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id       SERIAL PRIMARY KEY,
                name     VARCHAR(100) NOT NULL UNIQUE,
                nickname VARCHAR(100) NOT NULL
            );
        """)
        conn.commit()
        cur.execute("SELECT 1 FROM users WHERE name = %s", (name,))
        if cur.fetchone():
            cur.execute("UPDATE users SET nickname = %s WHERE name = %s", (nickname, name))
            msg = f"已更新 {name} 的暱稱為 {nickname}。"
        else:
            cur.execute("INSERT INTO users (name, nickname) VALUES (%s, %s)", (name, nickname))
            msg = f"成功註冊 {name}，暱稱：{nickname}。"
        conn.commit()
        conn.close()
        return jsonify({"message": msg}), 200
    except Exception as e:
        app.logger.error(f"[PostgreSQL] register error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500


@app.route("/api/postgres/search", methods=["GET"])
def pg_search():
    """從 Azure PostgreSQL 查詢 name 對應的 nickname"""
    name = (request.args.get("name", "") or "").strip()
    if not name:
        return jsonify({"message": "請提供姓名參數"}), 400

    try:
        conn = get_pg_conn()
        cur  = conn.cursor()
        cur.execute("SELECT nickname FROM users WHERE name = %s", (name,))
        row = cur.fetchone()
        conn.close()
        if row:
            return jsonify({"nickname": row[0]}), 200
        return jsonify({"message": f"找不到名為「{name}」的使用者。"}), 404
    except Exception as e:
        app.logger.error(f"[PostgreSQL] search error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500


# ════════════════════════════════════════════════════════════════════════
#  API：Cosmos DB (MongoDB API)
# ════════════════════════════════════════════════════════════════════════

@app.route("/api/cosmos-mongo/register", methods=["POST"])
def mongo_register():
    """新增 name + nickname 到 Cosmos DB (MongoDB API)"""
    data     = request.get_json(force=True)
    name     = (data.get("name", "") or "").strip()
    nickname = (data.get("nickname", "") or "").strip()

    if not name or not nickname:
        return jsonify({"message": "姓名與暱稱不能為空"}), 400

    try:
        col = get_mongo_col()
        existing = col.find_one({"name": name})
        if existing:
            col.update_one({"name": name}, {"$set": {"nickname": nickname}})
            msg = f"已更新 {name} 的暱稱為 {nickname}。"
        else:
            col.insert_one({"name": name, "nickname": nickname})
            msg = f"成功註冊 {name}，暱稱：{nickname}。"
        return jsonify({"message": msg}), 200
    except Exception as e:
        app.logger.error(f"[Cosmos MongoDB] register error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500


@app.route("/api/cosmos-mongo/search", methods=["GET"])
def mongo_search():
    """從 Cosmos DB (MongoDB API) 查詢 name 對應的 nickname"""
    name = (request.args.get("name", "") or "").strip()
    if not name:
        return jsonify({"message": "請提供姓名參數"}), 400

    try:
        col = get_mongo_col()
        doc = col.find_one({"name": name})
        if doc:
            return jsonify({"nickname": doc["nickname"]}), 200
        return jsonify({"message": f"找不到名為「{name}」的使用者。"}), 404
    except Exception as e:
        app.logger.error(f"[Cosmos MongoDB] search error: {e}")
        return jsonify({"message": f"資料庫錯誤：{str(e)}"}), 500


# ════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(debug=True, port=5000)
