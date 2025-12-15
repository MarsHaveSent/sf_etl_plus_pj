import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from datetime import datetime

class EmailSender:
    
    def __init__(self, logger, from_email, password, smtp_server, smtp_port=465):
        """
        Инициализация отправителя email
        
        Args:
            logger: экземпляр логгера ProjectLogger
            from_email: email отправителя (Системный аккаунт)
            password: пароль от email отправителя (Пароль от системного аккаунта!!!)
            smtp_server: SMTP сервер (например, smtp.mail.ru)
            smtp_port: порт SMTP сервера (по умолчанию 465 для SSL)
        """
        self.logger = logger
        self.from_email = from_email
        self.password = password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.context = ssl.create_default_context()
        
        self.logger.info(f"EmailSender initialized for {from_email}")
    
    def _send_email(self, to_email, subject, body):
        """
        Отправка email сообщения
        
        Args:
            to_email: email получателя
            subject: тема письма
            body: текст письма
            
        Returns:
            bool: успешно ли отправлено письмо
        """
        try:
            self.logger.info(f"Preparing email to {to_email} with subject: {subject}")
            
            # Создаем multipart сообщение 
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = to_email
            
            # Добавляем текст письма
            text_part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(text_part)
            
            # Отправляем письмо через SSL соединение
            self.logger.debug(f"Connecting to SMTP server {self.smtp_server}:{self.smtp_port}")
            
            with smtplib.SMTP_SSL(
                self.smtp_server, 
                self.smtp_port, 
                context=self.context
            ) as server:
                server.login(self.from_email, self.password)
                server.send_message(msg)
            
            self.logger.info(f"Email successfully sent to {to_email}")
            return True
            
        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP error during email sending: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error during email sending: {e}")
            return False
    
    def _get_script_duration(self, log_dir="logs"):
        """
        Получение времени выполнения скрипта из лог файла
        
        Args:
            log_dir: директория с логами
            
        Returns:
            tuple: (время начала, продолжительность) или (None, None)
        """
        try:
            # Получаем текущую дату для имени файла
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_file = os.path.join(log_dir, f"{current_date}.log")
            
            if not os.path.exists(log_file):
                return None, None
            
            with open(log_file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
            
            # Парсим дату из первой строки лога
            try:
                date_str = first_line[:19]  # Берем первые 19 символов "YYYY-MM-DD HH:MM:SS"
                start_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                end_time = datetime.now()
                
                # Вычисляем продолжительность
                duration_seconds = (end_time - start_time).total_seconds()
                hours, remainder = divmod(int(duration_seconds), 3600)
                minutes, seconds = divmod(remainder, 60)
                duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                
                return start_time, duration_str
                
            except:
                return None, None
                
        except Exception as e:
            self.logger.warning(f"Could not get script duration from logs: {e}")
            return None, None
    
    def _format_statistics(self, statistics):
        """
        Краткое форматирование статистики для email
        
        Args:
            statistics: словарь со статистикой
            
        Returns:
            str: краткая статистика
        """
        try:
            lines = []
            lines.append("ОСНОВНЫЕ МЕТРИКИ:")
            lines.append("-" * 30)
            
            # Основные метрики
            if 'total_records' in statistics:
                lines.append(f"Всего записей: {statistics['total_records']:,}")
            if 'unique_users' in statistics:
                lines.append(f"Уникальных пользователей: {statistics['unique_users']:,}")
            if 'submit_attempts' in statistics:
                lines.append(f"Submit попыток: {statistics['submit_attempts']:,}")
            if 'run_attempts' in statistics:
                lines.append(f"Run попыток: {statistics['run_attempts']:,}")
            if 'correct_attempts' in statistics:
                lines.append(f"Корректных: {statistics['correct_attempts']:,}")
            if 'incorrect_attempts' in statistics:
                lines.append(f"Некорректных: {statistics['incorrect_attempts']:,}")
            if 'success_rate' in statistics:
                lines.append(f"Процент успешных: {statistics['success_rate']:.1f}%")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"Error formatting statistics: {e}")
            return "Статистика недоступна"
    
    def send_simple_notification(self, to_email, script_name="Скрипт ETL"):
        """
        Отправка простого уведомления о завершении работы скрипта
        
        Args:
            to_email: email получателя
            script_name: название скрипта
            
        Returns:
            bool: успешно ли отправлено письмо
        """
        try:
            self.logger.info(f"Preparing simple notification to {to_email}")
            
            # Получаем время выполнения
            start_time, duration = self._get_script_duration()
            
            subject = f"INFO {script_name} - Завершен"
            
            body = f"""
                     Отчет о выполнении скрипта

                    Скрипт: {script_name}
                    Статус: УСПЕШНО ЗАВЕРШЕН

                    """
            
            if start_time and duration:
                body += f"""
                         Время начала: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
                         Продолжительность: {duration}\n\n
                         """
            else:
                body += f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            body += f"""
                    ---
                    Автоматическое уведомление. Не отвечайте на это письмо.
                    """
            
            return self._send_email(to_email, subject, body)
            
        except Exception as e:
            self.logger.error(f"Error preparing simple notification: {e}")
            return False
    
    def send_statistics_notification(self, to_email, statistics, script_name="Скрипт ETL"):
        """
        Отправка уведомления со статистикой
        
        Args:
            to_email: email получателя
            statistics: словарь со статистикой
            script_name: название скрипта
            
        Returns:
            bool: успешно ли отправлено письмо
        """
        try:
            self.logger.info(f"Preparing statistics notification to {to_email}")
            
            # Получаем время выполнения
            start_time, duration = self._get_script_duration()
            
            # Форматируем статистику
            stats_text = self._format_statistics(statistics)
            
            subject = f" INFO {script_name} - Отчет со статистикой"
            
            body = f"""
                    Отчет о выполнении скрипта

                    Скрипт: {script_name}
                    Статус: УСПЕШНО ЗАВЕРШЕН

                    """
            
            if start_time and duration:
                body += f"""
                         Время начала: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
                         Продолжительность: {duration}\n\n
                         """
            else:
                body += f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            body += f"""{stats_text}

                    ---
                    Автоматическое уведомление. Не отвечайте на это письмо.
                    """
            
            return self._send_email(to_email, subject, body)
            
        except Exception as e:
            self.logger.error(f"Error preparing statistics notification: {e}")
            return False
    
    def send_error_notification(self, to_email, error_message, script_name="Скрипт ETL"):
        """
        Отправка уведомления об ошибке
        
        Args:
            to_email: email получателя
            error_message: сообщение об ошибке
            script_name: название скрипта
            
        Returns:
            bool: успешно ли отправлено письмо
        """
        try:
            self.logger.info(f"Preparing error notification to {to_email}")
            
            subject = f"ERROR {script_name} - Ошибка выполнения"
            
            body = f"""
                    ОШИБКА ВЫПОЛНЕНИЯ СКРИПТА

                    Скрипт: {script_name}
                    Статус: ЗАВЕРШЕН С ОШИБКОЙ
                    Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

                    Ошибка: {error_message}

                    Лог файл: logs/{datetime.now().strftime('%Y-%m-%d')}.log

                    Требуются действия:
                    1. Проверить доступность сервера
                    2. Проверить подключение к базе данных
                    3. Проверить доступ к API

                    ---
                    Автоматическое уведомление. Не отвечайте на это письмо.
                    """
            
            return self._send_email(to_email, subject, body)
            
        except Exception as e:
            self.logger.error(f"Error preparing error notification: {e}")
            return False