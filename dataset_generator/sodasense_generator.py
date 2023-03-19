import pandas as pd
import numpy as np
import random
import datetime
from faker import Faker
from random import randint
from unidecode import unidecode
from datetime import datetime
from datetime import date
from datetime import timedelta
from wonderwords import RandomSentence

#faker=Faker(locale='el_GR')
faker = Faker()

df_product = pd.read_csv('../resources/scraped_data/sodasense/product.csv')
df_area = pd.read_csv('../resources/scraped_data/sodasense/area.csv')
df_event = pd.read_csv('../resources/scraped_data/sodasense/event.csv')

numberOfRows = 1000
generated_record = dict()

#Initialize random sentece generator
s = RandomSentence()


def generate_date(fromDate, toDate):
    date = faker.date_between(fromDate, toDate)
    return date


def generate_user_info():
    gender = np.random.choice(["M", "F"], p=[0.5, 0.5])
    if(gender=='M'):
        generated_record['firstName'] = faker.first_name_male()
        generated_record['lastName'] = faker.last_name()
        generated_record['email'] = generated_record['firstName'] + '.' + generated_record['lastName'] + '@gmail.com'
        return 0
    generated_record['firstName'] = faker.first_name()
    generated_record['lastName'] = faker.last_name()
    generated_record['email'] = generated_record['firstName'] + '.' + generated_record['lastName'] + '@gmail.com'
    return 0


def generate_measurement_info():
    generated_record['product'] = df_product.sample()['Product'].iloc[0]
    area_info = df_area.sample()
    generated_record['area'] = area_info['Area'].iloc[0]
    generated_record['postalCode'] = area_info['Postal Code'].iloc[0]
    generated_record['longtitude'] = area_info['Latitude'].iloc[0]
    generated_record['latitude'] = area_info['Longtitide'].iloc[0]
    generated_record['event'] = df_event.sample()['Event'].iloc[0]
    generated_record['description'] = s.sentence()


def main():
    produced_dataset = pd.DataFrame()

    for x in range(numberOfRows):
        generate_user_info()
        generate_measurement_info()
        produced_dataset = produced_dataset.append(generated_record, ignore_index=True)

    produced_dataset.to_csv('../resources/generator_output/custom_sodasense_dataset.csv', sep=',', index=False)


if __name__ == "__main__":
    main()