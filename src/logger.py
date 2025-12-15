import os
import logging
import glob
from datetime import datetime, timedelta

class ProjectLogger:

    __instance = None

    @staticmethod
    def get_instance():
        if not ProjectLogger.__instance:
            ProjectLogger()
        return ProjectLogger.__instance


    def __init__(self, log_dir="logs"):
        if ProjectLogger.__instance:
            raise Exception("This class is Singleton")
        else:
            self.log_dir = log_dir
            self.setup_logging()
            ProjectLogger.__instance = self
    
    
    def setup_logging(self):
        """Настройка логирования: DEBUG в файл, INFO+ в консоль"""
        # Создаем папку для логов
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Удаляем старые логи
        self.clean_old_logs()
        
        # Имя файла лога
        log_file = f"{self.log_dir}/{datetime.now().date()}.log"
        
        # Получаем корневой логгер
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Принимаем все уровни начиная с DEBUG
        
        # Очищаем существующие обработчики
        root_logger.handlers.clear()
        
        # 1. ФАЙЛОВЫЙ обработчик (записывает ВСЕ, включая DEBUG)
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Записываем с уровня DEBUG
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # 2. КОНСОЛЬНЫЙ обработчик (выводит только INFO и выше)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Выводим только с уровня INFO
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Логируем начало работы
        root_logger.info(f"Logging initialized. Log file: {log_file}")
        root_logger.debug(f"DEBUG messages will be written to {log_file}")
    
    def clean_old_logs(self, days_to_keep=3):
        """Удаляем логи старше N дней"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        for log_file in glob.glob(f"{self.log_dir}/*.log"):
            # Парсим дату из имени файла
            try:
                file_date_str = os.path.basename(log_file).replace('.log', '')
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                
                if file_date < cutoff_date:
                    os.remove(log_file)
                    print(f"[ProjectLogger] Old lof has been deleted: {log_file}")  # Используем print
            except ValueError:
                continue

    
    def info(self, message):
        logging.info(message)
    
    def error(self, message):
        logging.error(message)

    def warning(self,message):
        logging.warning(message)
    
    def debug(self,message):
        logging.debug(message)
