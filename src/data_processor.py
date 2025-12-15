from datetime import datetime

class DataProcessor:
    @staticmethod
    def process_data(unp_data,logger_cls):
        """Валидация данных полученных по API """

        logger_cls.info(f'Start of data processing. Recieved rows: {len(unp_data)}')
        
        processed_data = []

        for row in unp_data:
            #Валидация  id
            if not isinstance(row['user_id'],str):
                logger_cls.debug('Invalid user_id')
                continue

            # Валидация oauth_consumer_key (Раскомментировать при необходимсоти валидации)
            #if not row['oauth_consumer_key']:
            #    logger_cls.debug('Invalid oauth_consumer_key')
            #    continue

            #Валидация lis_result_sourcedid
            if not (isinstance(row['lis_result_sourcedid'],str) and 'course' in row['lis_result_sourcedid']):
                logger_cls.debug('Invalid "lis_result_sourcedid"')
                continue

            #Валидация lis_outcome_service_url
            if not (isinstance(row['lis_outcome_service_url'],str) and 'https' in row['lis_outcome_service_url']):
                logger_cls.debug('Invalid "lis_outcome_service_url"')
                continue

            #Валидация is_correct и приведение к булеву формату
            if (row['is_correct'] in (None,0,1)):
                row['is_correct'] = bool(row['is_correct']) 
            else:
                logger_cls.debug('Invalid "is_correct"')
                continue

            #Валидация attempt_type
            if not (row['attempt_type'] in ('submit','run')):
                logger_cls.debug('Invalid "attempt_type"')
                continue

            #Валидация формата даты и приведение к формату datetime 
            format_str="%Y-%m-%d %H:%M:%S.%f" 
            try:
                row['created_at'] = datetime.strptime(row['created_at'], format_str)
            except :
                logger_cls.debug('Invalid "created_at"')
                continue
            
            processed_data.append(row)
        
        logger_cls.info(f'Data had been processed. Left rows: {len(processed_data)}, ({round(len(processed_data)*100/len(unp_data),2)}%) out of recieved. ')
        
        return processed_data 

    @staticmethod
    def get_statistics(processed_data,logger_cls):
        """Получение представления статистики по валидированным данным"""
        
        if not processed_data:
            logger_cls.warning("No data given for analysis")
            return None
        
        try:
            logger_cls.info(f"Statistics for {len(processed_data)} records")
            
            #счетчики
            total_records = len(processed_data)
            unique_users = set()
            submit_attempts = 0
            run_attempts = 0
            correct_attempts = 0
            incorrect_attempts = 0
            earliest_attempt = None
            latest_attempt = None
            
            # Собираем статистику
            for record in processed_data:
                # Уникальные пользователи
                unique_users.add(record['user_id'])
                
                # Типы попыток
                if record['attempt_type'] == 'submit':
                    submit_attempts += 1
                elif record['attempt_type'] == 'run':
                    run_attempts += 1
                
                # Корректность попыток
                is_correct = record['is_correct']
                if is_correct is True:
                    correct_attempts += 1
                elif is_correct is False:
                    incorrect_attempts += 1
                elif isinstance(is_correct, (int, float)):
                    # Обработка числовых значений (0/1)
                    if is_correct:
                        correct_attempts += 1
                    else:
                        incorrect_attempts += 1
                elif isinstance(is_correct, str):
                    # Обработка строковых значений
                    if is_correct.lower() in ('true', '1', 't', 'yes'):
                        correct_attempts += 1
                    else:
                        incorrect_attempts += 1
                
                # Даты попыток
                created_at = record['created_at']
                if isinstance(created_at, str):
                    # Если дата в строковом формате, преобразуем
                    try:
                        created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S.%f")
                    except ValueError:
                        try:
                            created_at = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            continue
                
                if created_at:
                    if earliest_attempt is None or created_at < earliest_attempt:
                        earliest_attempt = created_at
                    if latest_attempt is None or created_at > latest_attempt:
                        latest_attempt = created_at
            
            
            # Формируем результат
            stats = {
                'total_records': total_records,
                'unique_users': len(unique_users),
                'submit_attempts': submit_attempts,
                'run_attempts': run_attempts,
                'correct_attempts': correct_attempts,
                'incorrect_attempts': incorrect_attempts,
                'earliest_attempt': earliest_attempt,
                'latest_attempt': latest_attempt,
                # Дополнительная статистика для аналитики
                'success_rate': round(correct_attempts * 100 / submit_attempts, 2) if submit_attempts > 0 else 0,
                'run_to_submit_ratio': round(run_attempts / submit_attempts, 2) if submit_attempts > 0 else float('inf'),
                'avg_attempts_per_user': round(total_records / len(unique_users), 2) if unique_users else 0,
                'date_range_days': (latest_attempt - earliest_attempt).days if earliest_attempt and latest_attempt else 0
            }
            
            # Логируем основные метрики
            logger_cls.info(f"Data statistics:")
            logger_cls.info(f"  Total records: {total_records}")
            logger_cls.info(f"  Unique users: {len(unique_users)}")
            logger_cls.info(f"  Submit attempts: {submit_attempts}")
            logger_cls.info(f"  Run attempts: {run_attempts}")
            logger_cls.info(f"  Correct attempts: {correct_attempts}")
            logger_cls.info(f"  Incorrect: {incorrect_attempts}")
            
            return stats
            
        except Exception as e:
            logger_cls.error(f"Error during analysis of data: {e}")
            return None       