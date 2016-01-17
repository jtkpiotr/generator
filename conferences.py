from datetime import datetime
from datetime import timedelta
from random import randint, sample
from utils import *


def get_conferences_with_dates(cursor):
    cursor.execute("SELECT * FROM Conferences")
    data = list(map(lambda x: (x[0],x[1],x[2]), cursor.fetchall()))
    return data

def get_conferences_day_with_day(cursor):
    cursor.execute("SELECT * FROM [Conference Days]")
    data = list(map(lambda x: (x[0],x[2]), cursor.fetchall()))
    return data

def generate_faculties(connection, cursor):
    for faculty in get_list('data/faculties.txt'):
        cursor.callproc('CreateFaculty', (faculty,))
    connection.commit()


def generate_workshops(connection, cursor):
    faculties = get_table_ids(cursor, "Faculties")
    titles = get_list('data/workshops.txt')
    for title in titles:
        FacultyID = get_random_value(faculties)
        cursor.callproc('CreateWorkshop', (title, FacultyID))
    connection.commit()

def generate_pricings(connection, cursor):
    conferences = get_table_ids(cursor, "Conferences")
    for ConferenceID in conferences:
        DaysBefore = randint(5,7)
        Discount = str(randint(10,15)/100)
        cursor.callproc('CreatePricing',(ConferenceID, DaysBefore, Discount))
        DaysBefore = randint(15,17)
        Discount = str(randint(20,25)/100)
        cursor.callproc('CreatePricing',(ConferenceID, DaysBefore, Discount))
        DaysBefore = randint(25,35)
        Discount = str(randint(35,40)/100)
        cursor.callproc('CreatePricing',(ConferenceID, DaysBefore, Discount))
    connection.commit()


def generate_conferences(connection, cursor):
    titles = get_list('data/conferences.txt')
    countries = get_table_ids(cursor, "Countries")
    prices = (100,120,140,160,180,200,220,240,260,280,300)
    capacities = (100,110,120,130,140,150)
    student_discounts = (0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70)
    StartDate = datetime.strptime('2013 01 14', "%Y %m %d")
    now = datetime.now()
    while(StartDate < now):
        EndDate = StartDate + timedelta(days=randint(1,3))
        Title = get_random_value(titles)
        CountryID = get_random_value(countries)
        BasePrice = get_random_value(prices)
        Capacity = get_random_value(capacities)
        StudentDiscount = get_random_value(student_discounts)
        cursor.callproc('CreateConference', (StartDate,EndDate,Title,CountryID,BasePrice,Capacity,str(StudentDiscount)))
        StartDate += timedelta(days=randint(13,17))
    connection.commit()

def generate_conference_days(connection, cursor):
    conferences = get_conferences_with_dates(cursor)
    for conference in conferences:
        ConferenceID = conference[0]
        Day = datetime.strptime(conference[1], "%Y-%m-%d")
        EndDate = datetime.strptime(conference[2], "%Y-%m-%d")
        while(Day <= EndDate):
            cursor.callproc('CreateConferenceDay',(ConferenceID, Day))
            Day += timedelta(days=1)
        connection.commit()

def generate_conference_day_details(connection, cursor):
    conference_days = get_conferences_day_with_day(cursor)
    workshops = get_table_ids(cursor, 'Workshops')
    for conference_day in conference_days:
        ConferenceDayID = conference_day[0]
        picked_workshops = sample(workshops, randint(3,5))
        for WorkshopID in picked_workshops:
            Date = datetime.strptime(conference_day[1], "%Y-%m-%d") + timedelta(hours=randint(8,18))
            Price = randint(20,50)
            Capacity = randint(20,70)
            cursor.callproc("CreateConferenceDayDetails",(WorkshopID,ConferenceDayID,Date,Price,Capacity))
    connection.commit()
