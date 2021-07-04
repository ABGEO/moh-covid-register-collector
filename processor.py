import json
import logging
import os
import sqlite3

from http_request_randomizer.requests.proxy.requestProxy import RequestProxy


class Processor:
    def __init__(self):
        self.api_url = 'https://www.moh.gov.ge/covid-chart/register/index.php'
        self.connection = sqlite3.connect('data/database.db')
        self.last_year = int(self.__get_meta('last_year'))
        self.last_code = self.__get_meta('last_code')
        self.last_id = int(self.__get_meta('last_id'))
        self.logger = self.__setup_logger()

        self.__initialize_proxy()

    def __setup_logger(self):
        if not os.path.exists('logs'):
            os.makedirs('logs')

        handler = logging.FileHandler('logs/app.log')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

        logger = logging.getLogger('app')
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        return logger

    def __get_meta(self, key):
        cursor = self.connection.execute('SELECT value FROM meta WHERE key = :key;', {'key': key})

        return cursor.fetchone()[0]

    def __update_meta(self, key, value):
        self.connection.execute('UPDATE meta set value = :value WHERE key = :key;', {'value': value, 'key': key})
        self.connection.commit()

    def __initialize_proxy(self):
        self.logger.info('Initialising proxy')
        self.req_proxy = RequestProxy(log_level=logging.ERROR)
        self.logger.info("Size: {0}".format(len(self.req_proxy.get_proxy_list())))

    def insert_person(self, personal_number, person):
        cursor = self.connection.cursor()

        cursor.execute(
            'INSERT INTO people (personal_number, first_name, last_name, date_of_birth) '
            'VALUES (:personal_number, :first_name, :last_name, :date_of_birth);',
            {
                'personal_number': personal_number,
                'first_name': person['FirstName'],
                'last_name': person['LastName'],
                'date_of_birth': person['DateOfBirth'],
            }
        )

        self.connection.commit()
        person_id = cursor.lastrowid

        self.logger.info(f'New person with PN {personal_number} has been saved at ID {person_id}.')

        return person_id

    def get_personal_info(self, year, personal_number):
        while True:
            if len(self.req_proxy.proxy_list) == 1:
                self.__initialize_proxy()

            response = self.req_proxy.generate_proxied_request(
                url=self.api_url,
                method='POST',
                data={
                    'PN': personal_number,
                    'PatientBirthYear': year,
                    'part': 'personal',
                },
                req_timeout=5
            )

            if response is not None:
                response = json.loads(response.content)
                if response['Result'] == 1:
                    return response

                return None

            self.logger.info("Proxy List Size: {0}".format(len(self.req_proxy.get_proxy_list())))

    def process(self):
        for n in range(self.last_id, 1000000000):
            if n % 10 == 0:
                self.__update_meta('last_id', n)

            personal_number = "{0}{1}{2}".format(self.last_code, ('0' * (9 - len(str(n)))), n)
            self.logger.info(f'Current PN is {personal_number}')

            try:
                person = self.get_personal_info(self.last_year, personal_number)
                if person:
                    self.insert_person(personal_number, person)
            except Exception as e:
                self.logger.error(e)
