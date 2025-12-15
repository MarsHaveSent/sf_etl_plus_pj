# Аналитика использования грейдера онлайн-университета

Скрипт для получения данных из API, обработки и сохранения в PostgreSQL.

## Структура проекта
project/
├── src/ # Исходный код
├── logs/ # Логи выполнения
├── config/ # Примеры заолнения файлов для работы
├── requirements.txt
└── README.md

## Установка

1. Клонировать репозиторий
2. Создать виртуальное окружение: `python -m venv venv`
3. Активировать окружение:
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
4. Установить зависимости: `pip install -r requirements.txt`
5. Заполнить данные по примеру secrets.env.example в свой файл .env
6. Подгрузить\заполнить credintials.json файл в папке config по примеру из credentials.json.example

## Запуск

```bash
python src/main.py