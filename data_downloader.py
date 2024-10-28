import sys
import requests
from datetime import datetime, timedelta
import zipfile
import gzip
import os
import shutil
import subprocess
import schedule
import time

def download_file(url, file_name):
    temp_file = file_name + ".part"
    resume_header = {}

    if os.path.exists(temp_file):
        resume_header = {'Range': f'bytes={os.path.getsize(temp_file)}-'}

    with open(temp_file, "ab") as f:
        response = requests.get(url, headers=resume_header, stream=True)
        total_length = response.headers.get('content-length')

        if total_length is None:
            f.write(response.content)
        else:
            dl = os.path.getsize(temp_file)
            total_length = int(total_length) + dl if 'Range' in response.headers else int(total_length)
            for data in response.iter_content(chunk_size=4096):
                dl += len(data)
                done = int(50 * dl / total_length)
                sys.stdout.write(f"\r[{'=' * done}{' ' * (50 - done)}]")
                sys.stdout.flush()
    os.rename(temp_file, file_name)

def unzip_file(zip_file, extract_to):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_file)

def decompress_gz_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".gz"):
                gz_file_path = os.path.join(root, file)
                output_file_path = os.path.join(root, file[:-3])
                with gzip.open(gz_file_path, 'rb') as gz_file:
                    with open(output_file_path, 'wb') as out_file:
                        out_file.write(gz_file.read())
                os.remove(gz_file_path)

def decompress_z_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".z") or file.endswith(".Z"):
                z_file_path = os.path.join(root, file)
                output_file_path = os.path.join(root, file[:-2])
                with open(z_file_path, 'rb') as f_in:
                    with open(output_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                if os.path.exists(z_file_path):
                    os.remove(z_file_path)

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def convert_crx_to_rnx(directory):
    crx_files = [f for f in os.listdir(directory) if f.endswith('.crx')]
    for crx_file in crx_files:
        crx_file_path = os.path.join(directory, crx_file)
        subprocess.run(['crx2rnx', crx_file_path], check=True)
        os.remove(crx_file_path)

def process_data():
    base_url = "https://example.com/"
    today = datetime.today()
    year = today.strftime("%Y")
    day_of_year = today.strftime("%j")
    url = f"{base_url}{year}/{day_of_year}/example_file.zip"
    ensure_directory_exists("rnx_files")
    download_file(url, "rnx_files/example_file.zip")
    unzip_file("rnx_files/example_file.zip", "rnx_files")
    decompress_gz_files("rnx_files")
    decompress_z_files("rnx_files")
    convert_crx_to_rnx("rnx_files")

schedule.every().day.at("03:00").do(process_data)

while True:
    schedule.run_pending()
    time.sleep(60)
