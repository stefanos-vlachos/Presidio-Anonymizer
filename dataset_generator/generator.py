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

#faker=Faker(locale='el_GR')
faker = Faker()

df_discount = pd.read_csv('../resources/scraped_data/discount_type.csv')
df_harbors = pd.read_csv('../resources/scraped_data/harbors.csv')
df_position = pd.read_csv('../resources/scraped_data/position.csv')
df_shippers = pd.read_csv('../resources/scraped_data/shippers.csv')
df_ships = pd.read_csv('../resources/scraped_data/ships_names.csv')

numberOfRows = 1000
generated_booking = dict()


def generate_harbor(avoidHarbor):
    while True:
        sampledHarbor = df_harbors.sample()['Harbor Name'].iloc[0]
        if(avoidHarbor != sampledHarbor):
            return sampledHarbor


def generate_date(fromDate, toDate):
    date = faker.date_between(fromDate, toDate)
    return date


def generate_travel_info():
    generated_booking['fromHarbor'] = generate_harbor("")
    generated_booking['toHarbor'] = generate_harbor(generated_booking['fromHarbor'])
    generated_booking['ship'] = df_ships.sample()['Ship Name'].iloc[0]
    generated_booking['shippingFirm'] = df_shippers.sample()['Firm'].iloc[0]
    generated_booking['passengers'] = randint(1,10)
    generated_booking['vehicles'] = randint(1, generated_booking['passengers']+1)
    generated_booking['position'] = df_position.sample()['Position'].iloc[0]
    generated_booking['seatNumber'] = randint(1,500)
    generated_booking['discount'] = df_discount.sample()['Type'].iloc[0]
    generated_booking['price'] = random.uniform(10.0, 100.0)

    generated_booking['departureDate'] = generate_date(date(1970,12,27), date(2023,12,31))
    generated_booking['departureTime'] = faker.time()
    generated_booking['arrivalTime'] = datetime.strptime(generated_booking['departureTime'], '%H:%M:%S') + timedelta(minutes=randint(60,1200))
    return 0


def generate_passenger_info():
    generated_booking['gender'] = np.random.choice(["M", "F"], p=[0.5, 0.5])
    generated_booking['birth_date'] = generate_date(date(1970,12,27), date(2004,12,27))
    generated_booking['nationality'] = faker.country()
    generated_booking['phone_number'] = faker.phone_number()

    if generated_booking['gender']=='M':
        generated_booking['firstName'] = faker.first_name_male()
        generated_booking['lastName'] = faker.last_name()
        generated_booking['email'] = generated_booking['firstName'] + '.' + generated_booking['lastName'] + '@gmail.com'
        return 0

    generated_booking['firstName'] = faker.first_name_female()
    generated_booking['lastName'] = faker.last_name()
    generated_booking['email'] = generated_booking['firstName'] + '.' + generated_booking['lastName'] + '@gmail.com'
    return 0


def generate_payment_info():
    generated_booking['cardNumber'] = faker.iban()
    generated_booking['creditName'] = generated_booking['firstName']
    generated_booking['creditSurame'] = generated_booking['lastName']
    generated_booking['creditExpiration'] = faker.credit_card_expire()
    return 0


def main():
    produced_dataset = pd.DataFrame()

    for x in range(numberOfRows):
        generate_travel_info()
        generate_passenger_info()
        generate_payment_info()
        produced_dataset = produced_dataset.append(generated_booking, ignore_index=True)

    produced_dataset.to_csv('../resources/generator_output/custom_dataset.csv', sep=',')


if __name__ == "__main__":
    main()