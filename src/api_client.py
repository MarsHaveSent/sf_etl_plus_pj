import requests
import json

class APIClient:
    
    def __init__(self, logger, base_url, client, client_key, str_dt_start, str_dt_end):
        """
        Инициализация API клиента 
        
        Args:
            logger: экземпляр логгера ProjectLogger
            base_url: url адрес отправки API запроса
            client: значение client для params в requests (регистр важен)
            client_key: значение client_key для params в requests (регистр важен)
            str_dt_start: дата-время начала в формате `2023-04-01 12:46:47.860798`` (время на сервере в нулевом часовом поясе)
            str_dt_end: аналогично дата окончания
        """
        self.url = base_url

        self.logger = logger

        self.params = {
        'client': client,
        'client_key': client_key,
        'start': str_dt_start,
        'end': str_dt_end
                }

    def request_data(self):
        """Выполняем запрос к API"""
        self.logger.info(f'Start of API request to {self.url}')
        
        try:
            response = requests.get(self.url, params=self.params)
            response.raise_for_status()
            self.response = response
        
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP error occurred for {self.url}: {http_err}")
        
        except requests.exceptions.ConnectionError as conn_err:
            self.logger.error(f"Connection error occurred for {self.url}: {conn_err}")
        
        except requests.exceptions.Timeout as time_err:
            self.logger.error(f"Timeout error occurred for {self.url}: {time_err}")
        
        except requests.exceptions.TooManyRedirects as redirect_err:
            self.logger.error(f"Too many redirects for {self.url}: {redirect_err}")
        
        except requests.exceptions.RequestException as err:
            # This will catch any other ambiguous errors
            self.logger.error(f"An ambiguous error occurred for {self.url}: {err}")
        else:
            self.data = response.json()
            if self.data:
                self.logger.info(f"Data gained successfully!: {response.status_code}")
            else:
                self.logger.warning(f'Couldn`t get data for {self.url} through json method! Don`t use .get_unpacked_data(), use .get_response() instead to handle correct unpacking!') 

    def get_response(self):
        return self.response

    def get_unpacked_data(self):
        """Получаем 'полские' данные"""
        self.logger.info(f"Start of unpacking data from {self.url}  Recieved rows: {len(self.data)}")

        unpacked_data=[]

        for row in self.data:
            
            task_dict = {}
            
            #Получение id пользователя
            try:
                task_dict['user_id'] = row['lti_user_id']
            except:
                self.logger.debug('Couldn`t get user_id from data! Try .get_response() for manual handling.')
                continue

            #Распаковка passback_params
            try:
                invalid_json_string = row['passback_params']
                valid_json_string = invalid_json_string.replace("'", '"')
                passback_params = json.loads(valid_json_string)
                
                task_dict['oauth_consumer_key'] = passback_params['oauth_consumer_key']
                task_dict['lis_result_sourcedid'] = passback_params['lis_result_sourcedid']
                task_dict['lis_outcome_service_url'] = passback_params['lis_outcome_service_url']
            except:
                self.logger.debug('Couldn`t unpack passback_params or get full information from data! Try .get_response() for manual handling.')
                continue

            #Получение метки is_correct
            try:
                task_dict['is_correct'] = row['is_correct']
            except:
                self.logger.debug('Couldn`t get "is_correct" from data! Try .get_response() for manual handling.')
                continue

            #Получение atempt_type
            try:
                task_dict['attempt_type'] = row['attempt_type']
            except:
                self.logger.debug('Couldn`t get "attempt_type" from data! Try .get_response() for manual handling.')
                continue

            #Получение created_at в формате datetime 
            try:
                task_dict['created_at'] = row['created_at']
            except:
                self.logger.debug('Couldn`t get "created_at" from data! Try .get_response() for manual handling.')
                continue
            
            unpacked_data.append(task_dict)
        
        self.logger.info(f"Data from {self.url} was unpacked. Unpacked rows: {len(unpacked_data)}, ({round(len(unpacked_data)*100/len(self.data),2)}%) out of recieved.")
        
        return unpacked_data    