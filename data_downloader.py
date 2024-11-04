import sys
import requests
from datetime import datetime, timedelta
import zipfile
import gzip
import os
import logging
import subprocess
import shutil
import schedule
import time

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "data_downloader.log")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])

logger = logging.getLogger("DataDownloader")

def download_file(url, file_name):
    temp_file = file_name + ".part"
    resume_header = {}

    if os.path.exists(temp_file):
        resume_header = {'Range': f'bytes={os.path.getsize(temp_file)}-'}

    with open(temp_file, "ab") as f:
        logger.info(f"Скачивание {file_name}")
        response = requests.get(url, headers=resume_header, stream=True)
        total_length = response.headers.get('content-length')

        if total_length is None:
            f.write(response.content)
        else:
            dl = os.path.getsize(temp_file)
            total_length = int(total_length) + dl if 'Range' in response.headers else int(total_length)
            for data in response.iter_content(chunk_size=4096):
                dl += len(data)
                f.write(data)
                done = int(50 * dl / total_length)
                sys.stdout.write(f"\r[{'=' * done}{' ' * (50 - done)}]")
                sys.stdout.flush()
    logger.info("\nСкачивание завершено")
    os.rename(temp_file, file_name)

def unzip_file(zip_file, extract_to):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info(f"Распаковано {zip_file} в {extract_to}/")
    os.remove(zip_file)
    logger.info(f"Удален {zip_file}")

def decompress_gz_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".gz"):
                gz_file_path = os.path.join(root, file)
                output_file_path = os.path.join(root, file[:-3])
                with gzip.open(gz_file_path, 'rb') as gz_file:
                    with open(output_file_path, 'wb') as out_file:
                        out_file.write(gz_file.read())
                logger.info(f"Декомпрессирован {gz_file_path} в {output_file_path}")
                os.remove(gz_file_path)
                logger.info(f"Удален {gz_file_path}")

def decompress_z_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".z") or file.endswith(".Z"):
                z_file_path = os.path.join(root, file)
                output_file_path = os.path.join(root, file[:-2])
                try:
                    with open(z_file_path, 'rb') as f_in:
                        with open(output_file_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    logger.info(f"Декомпрессирован {z_file_path} в {output_file_path}")
                except Exception as e:
                    logger.error(f"Ошибка декомпрессии {z_file_path}: {e}")
                else:
                    if os.path.exists(z_file_path):
                        os.remove(z_file_path)
                        logger.info(f"Удален {z_file_path}")

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def convert_crx_to_rnx(directory):
    crx2rnx_path = "CRX2RNX"
    if not shutil.which(crx2rnx_path):
        logger.error(f"Команда {crx2rnx_path} не найдена. Убедитесь, что инструмент установлен и доступен в PATH.")
        sys.exit(1)

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".crx"):
                crx_file_path = os.path.join(root, file)
                rnx_folder_name = file.split("_")[0]
                rnx_folder_path = os.path.join(directory, rnx_folder_name)

                ensure_directory_exists(rnx_folder_path)

                command = f"{crx2rnx_path} {crx_file_path} -f -d"
                try:
                    subprocess.run(command, check=True, shell=True)
                    rnx_file_path = crx_file_path.replace(".crx", ".rnx")
                    target_rnx_file_path = os.path.join(rnx_folder_path, os.path.basename(rnx_file_path))
                    shutil.move(rnx_file_path, target_rnx_file_path)
                    logger.info(f"Конвертирован {crx_file_path} в {target_rnx_file_path}")

                    if os.path.exists(crx_file_path):
                        os.remove(crx_file_path)
                        logger.info(f"Удален {crx_file_path} после успешной конвертации и перемещения")

                    receiver_name = rnx_folder_name
                    subprocess.Popen(['systemctl', 'start', f'receiver@{receiver_name}'])

                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка конвертации {crx_file_path}: {e}")
                except Exception as e:
                    logger.error(f"Ошибка перемещения файла {rnx_file_path} в {target_rnx_file_path}: {e}")

def process_existing_zip_files(data_dir, extract_to_dir):
    for file in os.listdir(data_dir):
        if file.endswith(".zip"):
            zip_file_path = os.path.join(data_dir, file)
            unzip_file(zip_file_path, extract_to_dir)
            decompress_gz_files(extract_to_dir)
            decompress_z_files(extract_to_dir)
            convert_crx_to_rnx(extract_to_dir)

def job():
    date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    link = f"https://api.simurg.space/datafiles/map_files?date={date}"
    file_name = f"{date}.zip"
    data_dir = "data_files"
    extract_to_dir = "rnx_files"

    ensure_directory_exists(data_dir)
    ensure_directory_exists(extract_to_dir)

    file_path = os.path.join(data_dir, file_name)

    if not os.path.exists(file_path):
        download_file(link, file_path)
    else:
        logger.info(f"Файл {file_path} уже существует, пропускаем загрузку")

    process_existing_zip_files(data_dir, extract_to_dir)
    logger.info("Все файлы успешно загружены, декомпрессированы и конвертированы")

if __name__ == "__main__":
    schedule.every().day.at("22:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(60)
