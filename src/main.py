#Подгрузка Библиотек
import os
from dotenv import load_dotenv
#Подгрузка модулей
from logger import ProjectLogger
from api_client import APIClient
from data_processor import DataProcessor
from database_handler import DatabaseHandler
from google_sheets import GoogleSheetsExporter
from email_sender import EmailSender
#Подгрузка переменных окружения 
load_dotenv()

def main():
    # 1. Инициализация логгера
    logger = ProjectLogger.get_instance()
    
    try:
        logger.info("=" * 50)
        logger.info("Start ETL process")
        logger.info("=" * 50)
        
        # 2. Получение данных из API
        logger.info("Step 1: Extract data from API")
        #Инициализация данных для API
        api_url = os.getenv('API_URL')
        client = os.getenv('API_CLIENT'),
        client_key = os.getenv('API_CLIENT_KEY'),
        start_dt =  os.getenv('START_DATE'),
        end_dt = os.getenv('END_DATE')
        
        api_client = APIClient(logger, api_url, client, client_key, start_dt, end_dt)
        
        api_client.request_data()
        raw_data = api_client.get_unpacked_data()
        
        if not raw_data:
            logger.error("No data for tarnsformation")
            return
        
        # 3. Обработка данных
        logger.info("Step 2: Data transform")
        processed_data = DataProcessor.process_data(raw_data, logger)
        
        if not processed_data:
            logger.error("Couldn`t process data")
            return
        
        # 4. Получение статистики
        logger.info("Step 3: Data statistics")
        statistics = DataProcessor.get_statistics(processed_data, logger)
        
        # 5. Загрузка в БД
        logger.info("Step 4: Load to PostgreSQL DB")
        db_params = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        db_handler = DatabaseHandler.get_instance(logger, db_params)
        
        if db_handler.test_connection():
            inserted_count = db_handler.insert_data(
                processed_data, 
                batch_size=int(os.getenv('BATCH_SIZE', 100))
            )
            logger.info(f"Records inseted: {inserted_count}")
        
        # 6. Экспорт в Google Sheets
        logger.info("Step 5: Export statistcs to Google Sheets")
        if statistics:
            sheets_exporter = GoogleSheetsExporter(
                logger=logger,
                credentials_file=os.getenv('GOOGLE_CREDENTIALS_FILE'),
                spreadsheet_id=os.getenv('GOOGLE_SPREADSHEET_ID')
            )
            
            sheets_exporter.export_stats(statistics)
        
        # 7. Отправка email уведомления
        logger.info("Step 6: Send email notification")
        email_sender = EmailSender(
            logger=logger,
            from_email=os.getenv('EMAIL_FROM'),
            password=os.getenv('EMAIL_PASSWORD'),
            smtp_server=os.getenv('EMAIL_SMTP_SERVER'),
            smtp_port=int(os.getenv('EMAIL_SMTP_PORT', 465))
        )
        
        # Выбираем тип уведомления
        if statistics:
            email_sender.send_statistics_notification(
                to_email=os.getenv('EMAIL_TO'),
                statistics=statistics,
                script_name="SF ETL Processor"
            )
        else:
            email_sender.send_simple_notification(
                to_email=os.getenv('EMAIL_TO'),
                script_name="SF ETL Processor"
            )
        
        logger.info("=" * 50)
        logger.info("ETL process was finished")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Critical Error: {e}")
        
        # Отправка уведомления об ошибке
        try:
            email_sender = EmailSender(
                logger=logger,
                from_email=os.getenv('EMAIL_FROM'),
                password=os.getenv('EMAIL_PASSWORD'),
                smtp_server=os.getenv('EMAIL_SMTP_SERVER'),
                smtp_port=int(os.getenv('EMAIL_SMTP_PORT', 465))
            )
            email_sender.send_error_notification(
                to_email=os.getenv('EMAIL_TO'),
                error_message=str(e),
                script_name="SF ETL Processor"
            )
        except:
            pass

if __name__ == "__main__":
    main()