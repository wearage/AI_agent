from flask import Flask, request, jsonify
import logging
import requests
import threading
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

# Очередь для хранения ответов
response_queue = []

# Сопоставление script_number с URL
SCRIPT_WEBHOOK_URLS = {
    "script1": "http://127.0.0.1:5001/process-response",  # URL для script1
    "script2": "http://127.0.0.1:5002/process-response",  # URL для script2
    # Добавьте другие скрипты по аналогии
}

@app.route('/response-webhook', methods=['POST'])
def process_response():
    """Получение ответа от сервера."""
    try:
        data = request.get_json()
        if not data or "username" not in data or "text" not in data or "script_number" not in data:
            logging.error("Некорректный запрос или отсутствуют ключи 'username', 'text' и 'script_number'")
            return jsonify({"error": "Invalid data"}), 400

        script_number = data["script_number"]
        username = data["username"]
        text = data["text"]

        logging.info(f"Получен ответ от сервера: script_number={script_number}, username={username}, text={text}")

        # Сохранение ответа в очередь
        response_queue.append({"script_number": script_number, "username": username, "text": text})

        return jsonify({"status": "processed"}), 200
    except Exception as e:
        logging.error(f"Ошибка обработки ответа от сервера: {e}")
        return jsonify({"error": "Ошибка обработки ответа"}), 500


def forward_responses():
    """Функция для пересылки данных из очереди на указанный URL."""
    while True:
        if response_queue:
            # Извлечение первого элемента из очереди
            response = response_queue.pop(0)
            try:
                script_number = response.get("script_number")
                target_url = SCRIPT_WEBHOOK_URLS.get(script_number)

                if not target_url:
                    logging.error(f"Неизвестный script_number: {script_number}")
                    continue

                # Отправка данных на указанный URL
                response_data = requests.post(target_url, json=response)
                response_data.raise_for_status()
                logging.info(f"Данные успешно отправлены в {script_number}: {response}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка пересылки данных: {e}")
        time.sleep(1)  # Задержка между отправками


if __name__ == "__main__":
    # Запуск пересылки в отдельном потоке
    threading.Thread(target=forward_responses, daemon=True).start()

    # Запуск Flask-сервера
    app.run(host="0.0.0.0", port=5000, debug=True)
