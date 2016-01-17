from utils import *
from conferences import get_table_ids
import random
import pymssql

def generate_clients(connection, cursor):
    emails = get_list("data/emails.txt")
    countries = get_table_ids(cursor, "Countries")
    cities = get_list("data/cities.txt")
    phones = get_list("data/phone_numbers.txt")
    for i in range(0,150):
        Email = get_random_value(emails)
        CountryID = get_random_value(countries)
        City = get_random_value(cities)
        Phone = get_random_value(phones)
        cursor.callproc('CreateClient',(Email,CountryID,City,Phone))
    connection.commit()

def generate_companies(connection, cursor):
    company_names = get_list("data/company_names.txt")
    clients = get_table_ids(cursor, "Clients")
    for ClientID, CompanyName in zip(clients, company_names):
        cursor.callproc('CreateCompany', (ClientID, CompanyName))
    connection.commit()

def generate_individuals(connection, cursor):
    first_names = get_list("data/first_names.txt")
    last_names = get_list("data/last_names.txt")
    clients = get_table_ids(cursor, "Clients")
    letters = ['1','2','3','4','5','6','7','8','9']
    for ClientID in clients:
        FirstName = get_random_value(first_names)
        LastName = get_random_value(last_names)
        StudentID = None
        if randint(1,10) == 5: StudentID = ''.join((random.choice(letters) for i in range(6)))
        try:
            cursor.callproc('CreateIndividualClient',(ClientID, FirstName, LastName, StudentID))
        except pymssql.DatabaseError as e:
            pass
    connection.commit()