import os
import uuid
from datetime import timedelta
from flask import Flask, request, jsonify, render_template, redirect
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_PUBLIC_HOST = os.getenv("MINIO_PUBLIC_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET_NAME = "storagefiles"



def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id SERIAL PRIMARY KEY,
        nama TEXT NOT NULL,
        email TEXT,
        file_url TEXT,
        filename TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()



minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def init_minio():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)


def upload_to_minio(file):

    object_name = f"{uuid.uuid4()}_{file.filename}"

    file.stream.seek(0, os.SEEK_END)
    file_size = file.stream.tell()
    file.stream.seek(0)

    minio_client.put_object(
        BUCKET_NAME,
        object_name,
        file.stream,
        length=file_size,
        content_type=file.content_type
    )

    return object_name

def delete_file(object_name):
    try:
        minio_client.remove_object(BUCKET_NAME, object_name)
    except S3Error:
        pass


@app.route("/users", methods=["POST"])
def create_item():
    allowed_extensions = {"png", "jpg", "jpeg"}

    def allowed_file(filename):
        return "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions

    nama = request.form.get("nama")
    email = request.form.get("email")
    file = request.files.get("file")

    if not file:
        return "no file", 400
    
    if not allowed_file(file.filename):
        return "Only png, jpg, jpeg allowed", 400
    
    if not file.mimetype.startswith("image/"):
        return "invalid image file", 400
    
    file_url = upload_to_minio(file)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO items (nama, email, file_url, filename)
        VALUES (%s,%s,%s,%s)
    """, (nama, email, file_url, file.filename))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/users", methods=["GET"])
def list_items():

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, nama, email, filename
        FROM items
        ORDER BY id DESC
    """)

    items = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(items)


@app.route("/users/<int:item_id>", methods=["GET"])
def list_id(item_id):

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM items WHERE id=%s", (item_id,))

    item = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify(item)

  
@app.route("/users/<int:item_id>", methods=["PUT"])
def update_item(item_id):

    nama = request.form.get("nama")
    email = request.form.get("email")
    file = request.files.get("file")

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT file_url FROM items WHERE id=%s", (item_id,))
    item = cur.fetchone()

    if not item:
        return redirect("/")

    object_name = item["file_url"]

    if file:

        if object_name:
            delete_file(object_name)

        object_name = upload_to_minio(file)

    cur.execute("""
        UPDATE items
        SET nama=%s,
            email=%s,
            file_url=%s
        WHERE id=%s
    """, (nama, email, object_name, item_id))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/users/<int:item_id>", methods=["DELETE"])
def delete_item(item_id):

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT file_url FROM items WHERE id=%s", (item_id,))
    item = cur.fetchone()

    if item and item["file_url"]:
        delete_file(item["file_url"])

    cur.execute("DELETE FROM items WHERE id=%s", (item_id,))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/")
def index():

    keyword = request.args.get("katakunci", "")

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if keyword:
        cur.execute("""
        SELECT * FROM items
        WHERE nama ILIKE %s OR email ILIKE %s
        ORDER BY id DESC
        """, (f"%{keyword}%", f"%{keyword}%"))
    else:
        cur.execute("SELECT * FROM items ORDER BY id DESC")

    items = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("index.html", items=items)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    init_db()
    init_minio()
    app.run(host="0.0.0.0", port=8080, debug=True)
