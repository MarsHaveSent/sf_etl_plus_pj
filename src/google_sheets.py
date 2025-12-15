import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

class GoogleSheetsExporter:
    
    def __init__(self, logger, credentials_file, spreadsheet_id, setup_columns=True):
        """
        Инициализация экспортера
        
        Args:
            logger: экземпляр логгера ProjectLogger
            credentials_file: путь к JSON файлу с учетными данными сервисного аккаунта
            spreadsheet_id: ID Google таблицы (из URL)
            setup_columns: флаг настройки ширины колонок
        """
        self.logger = logger
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id
        self.setup_columns = setup_columns
        self.client = None
        self.spreadsheet = None
        self.sheet = None
        self.sheet_name = "sf_statistics"
        
        # Порядок колонок для вывода
        self.column_order = [
            'date_loaded',
            'total_records',
            'unique_users', 
            'submit_attempts',
            'run_attempts',
            'correct_attempts',
            'incorrect_attempts',
            'success_rate',
            'run_to_submit_ratio',
            'avg_attempts_per_user',
            'date_range_days',
            'earliest_attempt',
            'latest_attempt'
        ]
        
        # Читаемые названия колонок
        self.column_names = {
            'date_loaded': 'Дата загрузки',
            'total_records': 'Всего записей',
            'unique_users': 'Уникальных пользователей',
            'submit_attempts': 'Submit попыток',
            'run_attempts': 'Run попыток',
            'correct_attempts': 'Корректных попыток',
            'incorrect_attempts': 'Некорректных попыток',
            'success_rate': 'Процент успешных',
            'run_to_submit_ratio': 'Соотношение Run/Submit',
            'avg_attempts_per_user': 'Среднее попыток на пользователя',
            'date_range_days': 'Дней в диапазоне',
            'earliest_attempt': 'Самая ранняя попытка',
            'latest_attempt': 'Самая поздняя попытка'
        }
        
        self._setup_client()
    
    def _setup_client(self):
        """Настройка клиента Google Sheets API"""
        try:
            self.logger.info("Setting up connection to Google Sheets API...")
            
            # Определяем области доступа
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Загружаем учетные данные
            creds = Credentials.from_service_account_file(
                self.credentials_file, 
                scopes=scopes
            )
            
            # Создаем клиент
            self.client = gspread.authorize(creds)
            
            # Открываем таблицу
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            self.logger.info(f"Connection to Google Sheets successful. Table: {self.spreadsheet.title}")
            
        except Exception as e:
            self.logger.error(f"Error during setup Google Sheets client: {e}")
            raise
    
    def _get_or_create_sheet(self):
        """Получение или создание листа для статистики"""
        try:
            self.sheet = self.spreadsheet.worksheet(self.sheet_name)
            self.logger.info(f"Sheet '{self.sheet_name}' was found")
            return True
        except gspread.exceptions.WorksheetNotFound:
            # Лист не найден, создаем новый
            self.logger.info(f"Sheet '{self.sheet_name}' not found, creating new one...")
            try:
                self.sheet = self.spreadsheet.add_worksheet(
                    title=self.sheet_name, 
                    rows=1000, 
                    cols=20
                )
                self.logger.info(f"Sheet '{self.sheet_name}' was created")
                return False  # Лист новый, пустой
            except Exception as e:
                self.logger.error(f"Error during sheet creation: {e}")
                raise
    
    def _is_sheet_empty(self):
        """Проверка, пустой ли лист"""
        try:
            # Получаем все значения
            all_values = self.sheet.get_all_values()
            
            # Если нет строк или первая строка пустая
            if not all_values or not any(all_values[0]):
                return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error during checking for empty sheet: {e}")
            return True
    
    def _set_column_widths(self, column_widths):
        """Настройка ширины колонок"""
        try:
            requests = []
            
            for col_letter, width_pixels in column_widths.items():
                # Создаем запрос на изменение ширины колонки
                request = {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": self.sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": ord(col_letter) - 65,  # A=0, B=1 и т.д.
                            "endIndex": ord(col_letter) - 64    # Конечный индекс (exclusive)
                        },
                        "properties": {
                            "pixelSize": width_pixels
                        },
                        "fields": "pixelSize"
                    }
                }
                requests.append(request)
            
            # Отправляем все запросы одним batch
            if requests:
                self.spreadsheet.batch_update({"requests": requests})
                self.logger.info(f"Column widths for {len(requests)} columns configured")
                
        except Exception as e:
            self.logger.warning(f"Could not set column widths: {e}")
    
    def _setup_sheet_header(self):
        """Настройка заголовка и заголовков колонок"""
        try:
            self.logger.info("Setting up sheet headers...")
            
            # 1. Устанавливаем основной заголовок
            self.sheet.update('A1', [["SF Statistics Dashboard"]])
            
            # Настраиваем форматирование заголовка
            self.sheet.format('A1', {
                "textFormat": {
                    "fontSize": 16,
                    "bold": True
                },
                "horizontalAlignment": "CENTER"
            })
            
            # Объединяем ячейки для заголовка
            merge_range = f'A1:{chr(65 + len(self.column_order) - 1)}1'
            self.sheet.merge_cells(merge_range)
            
            # 2. Устанавливаем заголовки колонок
            headers = [self.column_names[col] for col in self.column_order]
            self.sheet.update('A2', [headers])
            
            # 3. Настраиваем ширину колонок если включено
            if self.setup_columns:
                column_widths = {
                    'A': 220,   # Дата загрузки
                    'B': 180,   # Всего записей
                    'C': 250,   # Уникальных пользователей
                    'D': 180,   # Submit попыток
                    'E': 160,   # Run попыток
                    'F': 200,   # Корректных попыток
                    'G': 220,   # Некорректных попыток
                    'H': 190,   # Процент успешных
                    'I': 250,   # Соотношение Run/Submit
                    'J': 300,   # Среднее попыток на пользователя
                    'K': 200,   # Дней в диапазоне
                    'L': 280,   # Самая ранняя попытка
                    'M': 280    # Самая поздняя попытка
                }
                self._set_column_widths(column_widths)
            
            # 4. Форматирование заголовков колонок
            header_range = f'A2:{chr(65 + len(headers) - 1)}2'
            self.sheet.format(header_range, {
                "textFormat": {
                    "bold": True,
                    "fontSize": 11
                },
                "backgroundColor": {
                    "red": 0.95,
                    "green": 0.95,
                    "blue": 0.95
                },
                "borders": {
                    "top": {"style": "SOLID"},
                    "bottom": {"style": "SOLID"},
                    "left": {"style": "SOLID"},
                    "right": {"style": "SOLID"}
                },
                "horizontalAlignment": "CENTER",
                "wrapStrategy": "WRAP",
                "verticalAlignment": "MIDDLE"
            })
            
            # 5. Настраиваем высоту строки с заголовками
            try:
                self.spreadsheet.batch_update({
                    "requests": [{
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": self.sheet.id,
                                "dimension": "ROWS",
                                "startIndex": 1,  # Вторая строка
                                "endIndex": 2
                            },
                            "properties": {
                                "pixelSize": 50
                            },
                            "fields": "pixelSize"
                        }
                    }]
                })
            except Exception as e:
                self.logger.warning(f"Could not set row height: {e}")
            
            self.logger.info("Sheet headers configured successfully")
            
        except Exception as e:
            self.logger.error(f"Error during sheet headers configuration: {e}")
            raise
    
    def _prepare_stats_row(self, stats):
        """Подготовка строки данных из словаря статистики"""
        row = []
        
        # Добавляем текущую дату загрузки
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row.append(current_time)
        
        # Добавляем значения в порядке column_order
        for column in self.column_order[1:]:  # Пропускаем date_loaded
            value = stats.get(column)
            
            if value is None:
                row.append("")
            elif column in ['earliest_attempt', 'latest_attempt']:
                # Форматируем даты
                if isinstance(value, datetime):
                    row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    row.append(str(value))
            elif column in ['success_rate', 'run_to_submit_ratio', 'avg_attempts_per_user']:
                # Форматируем числа с плавающей точкой
                if isinstance(value, (int, float)):
                    row.append(f"{value:.2f}")
                else:
                    row.append(str(value))
            elif column == 'success_rate':
                # Для процента успешных
                if isinstance(value, (int, float)):
                    row.append(f"{value:.2f}")
                else:
                    row.append(str(value))
            else:
                row.append(str(value))
        
        return row
    
    def _find_next_empty_row(self):
        """Поиск следующей пустой строки для вставки данных"""
        try:
            # Получаем все значения в колонке A (дата загрузки)
            column_a = self.sheet.col_values(1)
            
            # Ищем первую пустую строку (начиная с 3й, так как 1-2 заголовки)
            for i in range(2, len(column_a) + 2):  # +2 потому что индексация с 1
                if i >= len(column_a) or column_a[i] == '':
                    return i + 1  # gspread использует 1-based индексацию
            
            # Если все строки заполнены, возвращаем следующую
            return len(column_a) + 1
            
        except Exception as e:
            self.logger.error(f"Error during finding correct row for insertion: {e}")
            return 3  # По умолчанию начинаем с 3й строки
    
    def _apply_data_formatting(self, row_index):
        """Применение форматирования к вставленным данным"""
        try:
            # Форматирование для числовых колонок
            numeric_columns = ['B', 'C', 'D', 'E', 'F', 'G', 'K']  # B-G, K
            for col in numeric_columns:
                self.sheet.format(f'{col}{row_index}', {
                    "horizontalAlignment": "RIGHT"
                })
            
            # Форматирование для процентных колонок
            percent_columns = ['H']  # Процент успешных
            for col in percent_columns:
                self.sheet.format(f'{col}{row_index}', {
                    "horizontalAlignment": "RIGHT"
                })
            
            # Форматирование для колонок с соотношениями
            ratio_columns = ['I', 'J']  # Соотношение Run/Submit и среднее
            for col in ratio_columns:
                self.sheet.format(f'{col}{row_index}', {
                    "horizontalAlignment": "RIGHT"
                })
            
            # Форматирование для дат
            date_columns = ['A', 'L', 'M']  # Дата загрузки, earliest, latest
            for col in date_columns:
                self.sheet.format(f'{col}{row_index}', {
                    "horizontalAlignment": "CENTER"
                })
            
            # Добавляем границы
            start_col = 'A'
            end_col = chr(65 + len(self.column_order) - 1)
            cell_range = f'{start_col}{row_index}:{end_col}{row_index}'
            
            self.sheet.format(cell_range, {
                "borders": {
                    "top": {"style": "SOLID"},
                    "bottom": {"style": "SOLID"},
                    "left": {"style": "SOLID"},
                    "right": {"style": "SOLID"}
                },
                "wrapStrategy": "WRAP",
                "verticalAlignment": "MIDDLE"
            })
            
        except Exception as e:
            self.logger.warning(f"Could not apply formatting to data: {e}")
    
    def _auto_adjust_column_widths(self):
        """Автоматическая подстройка ширины колонок под содержимое"""
        try:
            if not self.setup_columns:
                return
                
            self.logger.info("Auto-adjusting column widths...")
            
            # Получаем все данные
            all_values = self.sheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                return
            
            # Создаем запросы для настройки ширины
            requests = []
            
            for col_idx in range(min(len(self.column_order), len(all_values[1]))):
                max_len = 0
                # Проверяем все строки, включая заголовки
                for row_idx in range(len(all_values)):
                    if col_idx < len(all_values[row_idx]):
                        cell_value = str(all_values[row_idx][col_idx])
                        max_len = max(max_len, len(cell_value))
                
                # Рассчитываем ширину (примерно 10 пикселей на символ)
                width_pixels = min(max_len * 10 + 20, 400)
                width_pixels = max(width_pixels, 100)
                
                # Создаем запрос для этой колонки
                request = {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": self.sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1
                        },
                        "properties": {
                            "pixelSize": width_pixels
                        },
                        "fields": "pixelSize"
                    }
                }
                requests.append(request)
            
            # Применяем все запросы
            if requests:
                self.spreadsheet.batch_update({"requests": requests})
                self.logger.info(f"Auto-adjusted {len(requests)} column widths")
            
        except Exception as e:
            self.logger.warning(f"Could not auto-adjust column widths: {e}")
    
    def export_stats(self, stats_dict):
        """
        Экспорт статистики в Google Sheets
        
        Args:
            stats_dict: словарь со статистикой
            
        Returns:
            bool: успешно ли выполнена операция
        """
        try:
            self.logger.info("Starting export to Google Sheets...")
            
            # 1. Получаем или создаем лист
            sheet_exists = self._get_or_create_sheet()
            
            # 2. Проверяем, нужно ли настраивать заголовки
            if not sheet_exists or self._is_sheet_empty():
                self.logger.info("Sheet is empty, setting up headers...")
                self._setup_sheet_header()
            
            # 3. Подготавливаем данные
            data_row = self._prepare_stats_row(stats_dict)
            
            # 4. Находим следующую пустую строку
            next_row = self._find_next_empty_row()
            
            # 5. Вставляем данные
            self.logger.info(f"Inserting data in row {next_row}...")
            
            # Формируем диапазон для вставки
            start_col = 'A'
            end_col = chr(65 + len(data_row) - 1)
            update_range = f'{start_col}{next_row}:{end_col}{next_row}'
            
            self.sheet.update(update_range, [data_row])
            
            # 6. Применяем форматирование
            self._apply_data_formatting(next_row)
            
            # 7. Автоматически настраиваем ширину колонок
            self._auto_adjust_column_widths()
        
            # 8. Замораживаем заголовки (первые 2 строки)
            try:
                self.sheet.freeze(rows=2)
            except Exception as e:
                self.logger.warning(f"Could not freeze rows: {e}")
            
            self.logger.info(f"Statistics successfully exported to row {next_row}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during export to Google Sheets: {e}")
            return False