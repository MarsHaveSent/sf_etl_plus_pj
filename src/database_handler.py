import psycopg2
from psycopg2 import  Error
from psycopg2.extras import execute_batch
from datetime import datetime

class DatabaseHandler:
    __instance = None
    __connection = None
    
    @staticmethod
    def get_instance(logger=None, db_params=None):
        if not DatabaseHandler.__instance:
            DatabaseHandler(logger, db_params)
        return DatabaseHandler.__instance
    
    def __init__(self, logger=None, db_params=None, table_name="student_attempts"):
        if DatabaseHandler.__instance:
            raise Exception("This class is Singleton")
        else:
            self.logger = logger
            self.db_params = db_params
            self.table_name = table_name
            DatabaseHandler.__instance = self
    
    
    def _get_connection(self):
        """Получение соединения с БД (ленивая инициализация)"""
        if self.__connection is None or self.__connection.closed:
            try:
                self.logger.info(f"Connecting PostgreSQL - {self.db_params['host']}...")
                self.__connection = psycopg2.connect(**self.db_params)
                self.logger.info("Connect successfull!")
            except Error as e:
                self.logger.error(f"Error during connection to  PostgreSQL - {self.db_params['host']}: {e}")
                raise
        return self.__connection
    
    def _close_connection(self):
        """Закрытие соединения с БД"""
        if self.__connection and not self.__connection.closed:
            self.__connection.close()
            self.logger.debug(f"Connection with PostgreSQL - {self.db_params['host']}  - closed.")
    
    def __del__(self):
        self._close_connection()
    
    def _table_exists(self, cursor):
        """Проверка существования таблицы"""
        try:
            check_table_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """
            cursor.execute(check_table_query, (self.table_name,))
            return cursor.fetchone()[0]
        except Error as e:
            self.logger.error(f"Error during checking for existance of the table {self.table_name}: {e}")
            return False
    
    def _create_table(self, cursor):
        """Создание таблицы если не существует"""
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                oauth_consumer_key VARCHAR(255),
                lis_result_sourcedid TEXT NOT NULL,
                lis_outcome_service_url TEXT NOT NULL,
                is_correct BOOLEAN,
                attempt_type VARCHAR(10) NOT NULL CHECK (attempt_type IN ('run', 'submit')),
                created_at TIMESTAMP NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_attempt UNIQUE (user_id, lis_result_sourcedid, attempt_type, created_at)
            );
            
            -- Создаем индекс для быстрого поиска по user_id
            CREATE INDEX IF NOT EXISTS idx_user_id ON {self.table_name}(user_id);
            
            -- Создаем индекс для быстрого поиска по дате
            CREATE INDEX IF NOT EXISTS idx_created_at ON {self.table_name}(created_at);
            
            -- Создаем индекс для быстрого поиска по attempt_type
            CREATE INDEX IF NOT EXISTS idx_attempt_type ON {self.table_name}(attempt_type);
        """
        
        try:
            cursor.execute(create_table_query)
            self.logger.info(f"Table '{self.table_name}' created or already had been created!")
        except Error as e:
            self.logger.error(f"Error during table creation: {e}")
            raise
    
    def _check_duplicates(self, cursor, data_chunk):
        """Проверка на дубликаты перед вставкой"""
        # Создаем временную таблицу для проверки
        cursor.execute("""
            CREATE TEMP TABLE temp_attempts (
                user_id VARCHAR(255),
                lis_result_sourcedid TEXT,
                attempt_type VARCHAR(10),
                created_at TIMESTAMP
            ) ON COMMIT DROP;
        """)
        
        # Вставляем данные во временную таблицу
        insert_temp_query = """
            INSERT INTO temp_attempts (user_id, lis_result_sourcedid, attempt_type, created_at)
            VALUES (%s, %s, %s, %s);
        """
        
        temp_data = [
            (row['user_id'], row['lis_result_sourcedid'], 
             row['attempt_type'], row['created_at'])
            for row in data_chunk
        ]
        
        execute_batch(cursor, insert_temp_query, temp_data)
        
        # Проверяем какие записи уже существуют
        check_duplicates_query = f"""
            SELECT t.user_id, t.lis_result_sourcedid, t.attempt_type, t.created_at
            FROM temp_attempts t
            JOIN {self.table_name} a 
            ON t.user_id = a.user_id 
            AND t.lis_result_sourcedid = a.lis_result_sourcedid
            AND t.attempt_type = a.attempt_type
            AND t.created_at = a.created_at;
        """
        
        cursor.execute(check_duplicates_query)
        duplicates = cursor.fetchall()
        
        if duplicates:
            self.logger.warning(f"Найдено {len(duplicates)} дубликатов, они будут пропущены")
            # Фильтруем данные, оставляя только уникальные
            unique_data = []
            for row in data_chunk:
                key = (row['user_id'], row['lis_result_sourcedid'], 
                       row['attempt_type'], row['created_at'])
                if key not in [(d[0], d[1], d[2], d[3]) for d in duplicates]:
                    unique_data.append(row)
            return unique_data
        
        return data_chunk  # Все данные уникальны
    
    def insert_data(self, processed_data, batch_size=100):
        """
        Вставка обработанных данных в БД
        
        Args:
            processed_data: список словарей с обработанными данными
            batch_size: размер батча для вставки
        """
        if not processed_data:
            self.logger.warning("No data given to insert in DB!")
            return 0
        
        connection = None
        cursor = None
        inserted_count = 0
        
        try:
            self.logger.info(f"Start of inserting data. Total rows: {len(processed_data)}")
            start_time = datetime.now()
            
            # Получаем соединение
            connection = self._get_connection()
            cursor = connection.cursor()
            
            # Проверяем/создаем таблицу
            if not self._table_exists(cursor):
                self._create_table(cursor)
            
            # SQL запрос для вставки
            insert_query = f"""
                INSERT INTO {self.table_name} 
                (user_id, oauth_consumer_key, lis_result_sourcedid, 
                 lis_outcome_service_url, is_correct, attempt_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, lis_result_sourcedid, attempt_type, created_at) 
                DO NOTHING;
            """
            
            # Подготавливаем данные для вставки
            data_to_insert = []
            for row in processed_data:
                data_to_insert.append((
                    row['user_id'],
                    row['oauth_consumer_key'],
                    row['lis_result_sourcedid'],
                    row['lis_outcome_service_url'],
                    row['is_correct'],
                    row['attempt_type'],
                    row['created_at']
                ))
            
            # Вставляем данные батчами
            total_batches = (len(data_to_insert) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(data_to_insert))
                batch = data_to_insert[start_idx:end_idx]
                
                try:
                    execute_batch(cursor, insert_query, batch)
                    inserted_in_batch = cursor.rowcount
                    inserted_count += inserted_in_batch
                    
                    self.logger.debug(
                        f"Batch {batch_num + 1}/{total_batches}: "
                        f"inserted {inserted_in_batch} rows"
                    )
                    
                    connection.commit()
                    
                except Error as e:
                    connection.rollback()
                    self.logger.error(f"Error during batch insertion {batch_num + 1}: {e}")
                    # Можно продолжить с другими батчами
                    continue
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"End of data insertion. "
                f"Total inserts: {inserted_count} / rows: {len(processed_data)} . "
                f"Elapsed time: {elapsed_time:.2f} sec."
            )
            
            return inserted_count
            
        except Error as e:
            if connection:
                connection.rollback()
            self.logger.error(f"Critical error during work with db {self.db_params['host']}: {e}")
            raise
            
        finally:
            if cursor:
                cursor.close()
            # Не закрываем соединение полностью, чтобы использовать его повторно
            # Соединение закроется в деструкторе или при следующем вызове
    
    def get_stats(self):
        """Получение статистики по данным в таблице"""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(CASE WHEN attempt_type = 'submit' THEN 1 END) as submit_attempts,
                    COUNT(CASE WHEN attempt_type = 'run' THEN 1 END) as run_attempts,
                    COUNT(CASE WHEN is_correct = TRUE THEN 1 END) as correct_attempts,
                    COUNT(CASE WHEN is_correct = FALSE THEN 1 END) as incorrect_attempts,
                    MIN(created_at) as earliest_attempt,
                    MAX(created_at) as latest_attempt
                FROM {self.table_name};
            """
            
            cursor.execute(stats_query)
            result = cursor.fetchone()
            
            stats = {
                'total_records': result[0],
                'unique_users': result[1],
                'submit_attempts': result[2],
                'run_attempts': result[3],
                'correct_attempts': result[4],
                'incorrect_attempts': result[5],
                'earliest_attempt': result[6],
                'latest_attempt': result[7]
            }
            
            return stats
            
        except Error as e:
            self.logger.error(f"Error during stats get: {e}")
            return None
            
        finally:
            if cursor:
                cursor.close()
    
    def test_connection(self):
        """Тестирование подключения к БД"""
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            self.logger.info(f"Db connection was successful. PostgreSQL version: {db_version[0]}")
            cursor.close()
            return True
        except Error as e:
            self.logger.error(f"Conecction test was failed: {e}")
            return False