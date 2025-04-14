from flask import Flask, request, jsonify
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

app = Flask(__name__)

load_dotenv()

MINIO_ENDPOINT = str(os.getenv("MINIO_ENDPOINT"))
MINIO_ACCESS_KEY = str(os.getenv("MINIO_ACCESS_KEY"))
MINIO_SECRET_KEY = str(os.getenv("MINIO_SECRET_KEY"))
MINIO_BUCKET_NAME = str(os.getenv("MINIO_BUCKET_NAME"))

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=True
)

if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
    minio_client.make_bucket(MINIO_BUCKET_NAME)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected for uploading"}), 400

    allowed_extensions = {'pdf', 'doc', 'docx', 'txt'}
    if '.' not in file.filename or file.filename.rsplit('.', 1)[1].lower() not in allowed_extensions:
        return jsonify({"error": "File type not allowed. Only pdf, doc, docx, and txt are supported"}), 400

    try:
        file_path = os.path.join(MINIO_BUCKET_NAME, file.filename)
        minio_client.put_object(
            MINIO_BUCKET_NAME,
            file.filename,
            file.stream,
            length=-1,
            part_size=10 * 1024 * 1024 
        )
        return jsonify({"message": f"File '{file.filename}' uploaded successfully"}), 200
    except S3Error as e:
        return jsonify({"error": str(e)}), 500

@app.route('/files', methods=['GET'])
def list_files():
    try:
        objects = minio_client.list_objects(MINIO_BUCKET_NAME)
        file_list = [obj.etag for obj in objects]
        return jsonify({"files": file_list}), 200
    except S3Error as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete/<etag>', methods=['DELETE'])
def delete_file(etag):
    try:
        objects = minio_client.list_objects(MINIO_BUCKET_NAME)
        for obj in objects:
            if obj.etag == etag:
                minio_client.remove_object(MINIO_BUCKET_NAME, obj.object_name)
                return jsonify({"message": f"File with etag '{etag}' deleted successfully"}), 200
        return jsonify({"error": "File with the specified etag not found"}), 404
    except S3Error as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)