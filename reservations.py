from utils import *
from datetime import datetime
from datetime import timedelta
import random
from random import sample
from conferences import get_conferences_with_dates
from random import choice
from pymssql import DatabaseError

def get_inserted_id(cursor):
    cursor.execute("SELECT @@IDENTITY")
    return int(cursor.fetchall()[0][0])

def check(cursor):
    cursor.execute("SELECT dbo.IsIndividualClient(%s)", 4)
    return cursor.fetchall()

def get_reservaion_details_clientIDs_amounts(cursor):
    query = """
select ReservationDetailsID, ClientID, Amount from [Reservation Details] as RD
inner join [Conference Reservations] as CR
on RD.ConferenceReservationID = CR.ConferenceReservationID
"""
    cursor.execute(query)
    return cursor.fetchall()

def get_workshop_reservations_wih_details(cursor):
    cursor.execute("SELECT WorkshopReservationID, ReservationDetailsID, Amount from [Workshop Reservations]")
    return cursor.fetchall()

def get_paricipants_for_reservation_details(cursor, ReservationDetailsID):
    cursor.execute("select DeclarationID from [Participant Declarations] where ReservationDetailsID = %s", ReservationDetailsID)
    return list(map(lambda x: x[0],cursor.fetchall()))

def get_workshops_for_day(cursor, ConferenceDayID):
    cursor.execute("SELECT * FROM dbo.GetWorkshopsForDay(%s)",ConferenceDayID)
    return list(map(lambda x: x[0],cursor.fetchall()))

def get_all_reservation_details(cursor):
    cursor.execute("SELECT ReservationDetailsID, ConferenceDayID, Amount from [Reservation Details]")
    return list(map(lambda x: (x[0],x[1],x[2]), cursor.fetchall()))

def get_workshop_left_capacity(cursor, WorkshopID, ConferenceDayID):
    cursor.execute("SELECT dbo.GetWorkshopLeftCapacity(%s,%s)",(WorkshopID,ConferenceDayID))
    return int(cursor.fetchone()[0])

def get_conference_reservations_with_date(cursor):
    cursor.execute("SELECT ConferenceReservationID, ReservationDate from [Conference Reservations]")
    return cursor.fetchall()


def generate_conference_reservations(cursor, connection):
    conferences = get_conferences_with_dates(cursor)
    individuals = get_table_ids(cursor, "[Individual Clients]")
    companies = get_table_ids(cursor, "Companies")
    for conference in conferences:
        ConferenceID = conference[0]
        StartDate = datetime.strptime(conference[1], "%Y-%m-%d")

        individuals_num = randint(20,25)
        individuals_for_conference = random.sample(individuals, individuals_num)

        companies_num = randint(3,5)
        companies_for_conference = random.sample(companies, companies_num)

        individual_reservations = []
        company_reservations = []

        for IndividualID in individuals_for_conference:
            cursor.callproc("CreateConferenceReservation",(IndividualID, StartDate - timedelta(days=randint(15,35))))
            individual_reservations.append(get_inserted_id(cursor))
        for CompanyID in companies_for_conference:
            cursor.callproc("CreateConferenceReservation",(CompanyID, StartDate - timedelta(days=randint(15,35))))
            company_reservations.append(get_inserted_id(cursor))

        connection.commit()

        cursor.execute("SELECT ConferenceDayID from [Conference Days] where ConferenceID = %s",ConferenceID)
        conference_days = map(lambda x: x[0], cursor.fetchall())

        used_individuals = set()
        used_companies = set()
        for ConferenceDayID in conference_days:
            day_individuals = random.sample(individual_reservations, randint(5,20))
            for ReservationID in day_individuals:
                used_individuals.add(ReservationID)
                cursor.callproc("CreateReservationDetails",(ReservationID, ConferenceDayID,1))

            day_companies = random.sample(company_reservations, randint(0,3))
            for ReservationID in day_companies:
                used_companies.add(ReservationID)
                cursor.callproc("CreateReservationDetails",(ReservationID, ConferenceDayID,randint(5,20)))
        connection.commit()
        unused_individuals = set(individual_reservations) - used_individuals
        unused_companies = set(company_reservations) - used_companies
        for ReservationID in unused_individuals:
            cursor.execute("delete from [Conference Reservations] where ConferenceReservationID = %s", ReservationID)
        for ReservationID in unused_companies:
            cursor.execute("delete from [Conference Reservations] where ConferenceReservationID = %s", ReservationID)
        connection.commit()



    connection.commit()



def generate_workshop_reservations(cursor, connection):
    reservation_details = get_all_reservation_details(cursor)
    for reservation in reservation_details:
        ReservationDetailsID = reservation[0]
        ConferenceDayID = reservation[1]
        Reservation_Amount = reservation[2]
        workshops = get_workshops_for_day(cursor, ConferenceDayID)
        picked_workshops = sample(workshops, randint(1,len(workshops)))
        for WorkshopID in picked_workshops:
            Amount = randint(1,Reservation_Amount)
            try:
                cursor.callproc("CreateWorkshopReservation",(WorkshopID,ReservationDetailsID,Amount))
                connection.commit()
            except DatabaseError:
                pass

def generate_participant_declaration(cursor, connection):
    reservation_details = get_reservaion_details_clientIDs_amounts(cursor)
    first_names = get_list("data/first_names.txt")
    last_names = get_list("data/last_names.txt")
    for reservation_detail in reservation_details:
        Amount = reservation_detail[2]
        for x in range(0, Amount):
            ReservationDetailsID = reservation_detail[0]
            FirstName = choice(first_names)
            LastName = choice(last_names)
            Signature = FirstName[:3] + LastName[:5]
            cursor.callproc("CreateParticipantDeclaration",(ReservationDetailsID,FirstName,LastName,Signature))
    connection.commit()

def generate_workshop_declarations(cursor, connection):
    workshop_reservations = get_workshop_reservations_wih_details(cursor)
    for workshop_reservation in workshop_reservations:
        WorkshopReservationID = workshop_reservation[0]
        Amount = workshop_reservation[2]
        participants = get_paricipants_for_reservation_details(cursor, workshop_reservation[1])
        for x in range(0, Amount):
            DeclarationID = participants.pop()
            cursor.callproc("CreateWorkshopDeclaration",(WorkshopReservationID, DeclarationID))
    connection.commit()


def generate_payments(cursor, connection):
    conference_reservations = get_conference_reservations_with_date(cursor)
    for conference_reservation in conference_reservations:
        PaymentID = conference_reservation[0]
        ReservationDate = datetime.strptime(conference_reservation[1], "%Y-%m-%d")
        cursor.callproc("CreatePayment",(PaymentID, 1, ReservationDate + timedelta(days=randint(0,10))))
    connection.commit()
