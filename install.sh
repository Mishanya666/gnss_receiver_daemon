#!/bin/bash

# Установка зависимостей
echo "Установка зависимостей..."
pip install -r requirements.txt

# Запуск GNSS Receiver Daemon с аргументом по умолчанию
echo "Запуск GNSS Receiver Daemon..."
python3 gnss_receiver_daemon.py default_receiver
