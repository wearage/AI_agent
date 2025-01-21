import logging
import asyncio
from pyrogram import Client, filters
import requests
from flask import Flask, request, jsonify
import pandas as pd
import threading
import psycopg2

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Конфигурация
SESSION_FILE = "+79999999999"
N8N_WEBHOOK_URL = "https://example/webhook/example"
EXCEL_FILE = "/yuorpath/usernames.xlsx"
USERNAME_COLUMN = "username"
NAME_COLUMN = "name"
BATCH_SIZE = 5  # Максимальное количество отправлений за день
DB_CONFIG = {
    "dbname": " ",
    "user": " ",
    "host": "localhost",
    "port": 5432,
}

# Flask сервер для обработки ответов
app = Flask(__name__)
response_queue = []  # Очередь для ответов

# Инициализация Pyrogram
client = Client(SESSION_FILE)

# Глобальные переменные для буферизации сообщений
message_buffer = {}
message_timers = {}

# Подключение к PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def initialize_db():
    """Инициализация таблицы в БД."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sent_messages (
                    username VARCHAR(50) PRIMARY KEY,
                    responded BOOLEAN DEFAULT FALSE
                )
            """)
            conn.commit()

async def send_combined_message(username):
    """Отправка объединенного сообщения."""
    if username in message_buffer:
        combined_message = "\n".join(message_buffer[username])
        try:
            payload = {
                "script_number": "script1",  # Новый параметр
                "username": username,
                "text": combined_message
            }
            response = requests.post(N8N_WEBHOOK_URL, json=payload)
            response.raise_for_status()
            logging.info(f"Объединенное сообщение клиента {username} отправлено в N8N: {combined_message}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка отправки объединенного сообщения в N8N: {e}")
        finally:
            # Очистка буфера и таймера для пользователя
            message_buffer.pop(username, None)
            message_timers.pop(username, None)

@client.on_message(filters.private)
async def handle_message(client, message):
    """Обработка входящих сообщений от пользователей с буферизацией."""
    username = message.chat.username
    text = message.text

    if not username:
        logging.warning("Сообщение от пользователя без username. Пропуск.")
        return

    logging.info(f"Получено сообщение от клиента {username}: {text}")

    # Добавление сообщения в буфер
    if username not in message_buffer:
        message_buffer[username] = []
    message_buffer[username].append(text)

    # Сброс или установка таймера для отправки сообщений
    if username in message_timers:
        message_timers[username].cancel()

    message_timers[username] = asyncio.get_event_loop().call_later(
        15, lambda: asyncio.create_task(send_combined_message(username))
    )

@app.route('/process-response', methods=['POST'])
def process_response():
    """Обработка входящих ответов от клиентов."""
    try:
        data = request.get_json()
        if not data or "username" not in data or "text" not in data:
            logging.error("Некорректный запрос или отсутствуют ключи 'username' и 'text'")
            return jsonify({"error": "Invalid data"}), 400

        username = data["username"]
        text = data["text"]
        logging.info(f"Получен ответ от сервера: username={username}, text={text}")

        # Обновление статуса в БД
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE sent_messages
                    SET responded = TRUE
                    WHERE username = %s
                """, (username,))
                conn.commit()

        # Сохранение ответа в очередь
        response_queue.append({"username": username, "text": text})
        return jsonify({"status": "processed"}), 200

    except Exception as e:
        logging.error(f"Ошибка обработки ответа от сервера: {e}")
        return jsonify({"error": "Ошибка обработки ответа"}), 500

async def response_worker():
    """Обработка очереди ответов."""
    while True:
        if response_queue:
            response = response_queue.pop(0)
            username = response["username"]
            text = response["text"]
            logging.info(f"Отправка ответа в Telegram: username={username}, text={text}")

            try:
                await client.send_message(username, text)
            except Exception as e:
                logging.error(f"Ошибка отправки сообщения в Telegram: {e}")
        await asyncio.sleep(1)

async def send_welcome_messages():
    """Отправка приветственных сообщений."""
    try:
        df = pd.read_excel(EXCEL_FILE)
        if USERNAME_COLUMN not in df.columns or NAME_COLUMN not in df.columns:
            raise ValueError(f"Excel файл должен содержать колонки '{USERNAME_COLUMN}' и '{NAME_COLUMN}'")

        users = df[[USERNAME_COLUMN, NAME_COLUMN]].dropna().to_dict(orient="records")

        for user in users:
            username = user[USERNAME_COLUMN]
            name = user[NAME_COLUMN]
            message = f"{name}, здравствуйте!\n\nЭто ваше приветственное сообщение."

            try:
                await client.send_message(username, message)
                logging.info(f"Приветственное сообщение отправлено {username}")

                # Добавление записи в БД
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO sent_messages (username, responded)
                            VALUES (%s, FALSE)
                            ON CONFLICT (username) DO NOTHING
                        """, (username,))
                        conn.commit()

            except Exception as e:
                logging.error(f"Ошибка отправки приветственного сообщения {username}: {e}")

    except Exception as e:
        logging.error(f"Ошибка загрузки пользователей из таблицы: {e}")

def flask_server():
    """Запуск Flask-сервера."""
    app.run(host="0.0.0.0", port=5001, debug=False, use_reloader=False)

if __name__ == "__main__":
    initialize_db()
    threading.Thread(target=flask_server, daemon=True).start()
    client.start()
    loop = asyncio.get_event_loop()
    loop.create_task(send_welcome_messages())
    loop.create_task(response_worker())
    loop.run_forever()
