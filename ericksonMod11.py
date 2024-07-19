import mysql.connector
from mysql.connector import errorcode
from datetime import date

def display_remaining_vaccinatons_needed_for_each_customer(my_cursor):
    def get_query_result(query):
        my_cursor.execute(query)
        return my_cursor.fetchall()

    # Vaccinatons Needed Report
    # data needed --> (trip vaccionaion requirements, customers on excursions of those trip types, vaccinations those customers have)
    # Determine discrepancies/mismatches of vaccinatons required vs. vaccinatons obtained for each customer
    
    # This query gets all the vaccinatons required for a tripType
    vaccs_per_trip_query = """
    select trip_type.tripName, vaccinations.vaccinationName
    from trip_type
    left join required_trip_vaccinations on trip_type.id = required_trip_vaccinations.tripId
    left join vaccinations on required_trip_vaccinations.vaccinationId = vaccinations.id"""

    # This query gets all the trip types a customer is signed up for
    trip_type_per_customer = """
    select customers.lastName, trip_type.tripName
    from customers
    left join excursions on customers.excursionId = excursions.id
    left join trip_type on excursions.tripTypeId = trip_type.id;"""

    # This query gets all the vaccinations a customer has
    vaccs_per_customer_query = """
    select customers.lastName, vaccinations.vaccinationName
    from customers
    left join customer_vaccinations on customers.id = customer_vaccinations.customerId
    left join vaccinations on customer_vaccinations.vaccinationId = vaccinations.id;"""

    # Get relevant data
    vaccs_per_trip_data = get_query_result(vaccs_per_trip_query)
    trips_per_customer_data = get_query_result(trip_type_per_customer)
    vaccs_per_customer_data = get_query_result(vaccs_per_customer_query)

    # Function to organize data
    def group_data_into_dict(query_data):
        group_dict = {}
        for row in query_data:
            dict_keys = list(group_dict.keys())
            if row[0] not in dict_keys:
                group_dict[row[0]] = []
        for row in query_data:
            group_dict[row[0]].append(row[1])
        return group_dict

    # Organize data
    trip_vacc_dict = group_data_into_dict(vaccs_per_trip_data)
    customer_trip_dict = group_data_into_dict(trips_per_customer_data)
    customer_vacc_dict = group_data_into_dict(vaccs_per_customer_data)

    # Function that flattens two-dimentional lists into one-dimentional lists
    def flatten_extend(matrix):
        flat_list = []
        for row in matrix:
            flat_list.extend(row)
        return flat_list

    # Use the trips customers are on and the vaccinatons required by each trip to determine total vaccinatons required for each customer
    def generate_required_customer_vacc_dict(trip_vacc_dict, customer_trip_dict):
        # Build dictionary of customer keys with empty list values
        required_customer_vacc_dict = {}
        for customer in list(customer_trip_dict.keys()):
            required_customer_vacc_dict[customer] = []

        # Fill the list values for each customer key with all the required vaccinations for every tripType the customer is registerd for
        for customer in customer_trip_dict:
            for trip in trip_vacc_dict:
                if trip in customer_trip_dict[customer]:
                    required_customer_vacc_dict[customer].append(trip_vacc_dict[trip])

        # Each entry in needed_customer_vacc_dict is a list of lists, we want to flatten that into one list of total needed vaccinations
        for customer in required_customer_vacc_dict:
            required_customer_vacc_dict[customer] = flatten_extend(required_customer_vacc_dict[customer])

        # Return total required vaccinations for each customer
        return required_customer_vacc_dict
    
    # Get dictionary of customers and their corresponding total required vaccinatons
    required_customer_vacc_dict = generate_required_customer_vacc_dict(trip_vacc_dict, customer_trip_dict)

    # Compare the vaccinations each customer has with the vaccinations they need and return the remaining needed vaccinatons for each customer
    def generate_needed_customer_vacc_dict(customer_vacc_dict, required_customer_vacc_dict):
        # Prepare empty final dictionary to be returned
        needed_customer_vacc_dict = {}

        # The customers to be referenced
        customers = list(customer_vacc_dict.keys())

        # Continue preparing final dictionary by pairing each customer with an empty list of remaining required vaccinatons
        for customer in customers:
            needed_customer_vacc_dict[customer] = []

        # If a customer does not have a required vaccinaton, add it to their list of remaining required vaccinatons in the dictionary
        for customer in customers:
            for vacc in required_customer_vacc_dict[customer]:
                if vacc not in customer_vacc_dict[customer] and vacc != None:
                    needed_customer_vacc_dict[customer].append(vacc)
        
        # It is unnecessary to report that a customer has zero remaining needed vaccinations
        # We will omit these customers by removing them from the dictioanry

        # Prepare list of customers to remove
        customers_to_pop = []

        # If a customer has zero remaining needed vaccinations, add them to the "to be removed" list
        for customer in needed_customer_vacc_dict:
            if len(needed_customer_vacc_dict[customer]) == 0:
                customers_to_pop.append(customer)
        
        # Finally, for each customer in the "to be removed" list, remove them from the dictionary
        for customer in customers_to_pop:
            needed_customer_vacc_dict.pop(customer)

        return needed_customer_vacc_dict
    
    # Final dictionary of customers and their corresponding list of needed vaccinations that they don't have yet
    needed_customer_vacc_dict = generate_needed_customer_vacc_dict(customer_vacc_dict, required_customer_vacc_dict)

    # Function to display each customer and their remaining needed vaccinations. This is the report.
    def display_needed_customer_vaccs(needed_customer_vacc_dict):
        print("---DISPLAYING REMAINING NEEDED VACCINATONS TO FULFILL TRIP REQUIREMENTS---\n")
        for customer in needed_customer_vacc_dict:
            print(f"--Customer with last name of {customer} still needs the following vaccinations:")
            for vacc in needed_customer_vacc_dict[customer]:
                print(f"the {vacc.upper()} vaccination\n")

    # Use the display function to display report
    display_needed_customer_vaccs(needed_customer_vacc_dict)
# ---------
# Note: in the data, there is no customer who is going on two excursions, this makes this report less interesting
# Note: three out of five of the customers in the data are going to Fjords of Norway which doesn't require any vaccinations, this makes the report less interesting
# Consider: Changing the data to make the report more interesting?
#----------

# Report to track equipment age -- compare purchase date of each equipment unit to the present date
# Use this report to identify potentially dangerous equipment to be disposed of and to inform purchasing decisions for new equipment
def display_equipment_age_report(my_cursor):
    def get_query_result(query):
        my_cursor.execute(query)
        return my_cursor.fetchall()
    
    def is_older_than_five(purchase_date, today_date):
        days_difference = abs(today_date - purchase_date).days
        if days_difference > 1825:
            return True
        else:
            return False
    
    equipment_units_query = """
    select equipment_units.id as UnitID, equipment.equipmentName, equipment_units.purchaseDate
    from equipment_units
    left join equipment on equipment_units.equipmentId = equipment.id;"""

    equipment_units_data = get_query_result(equipment_units_query)

    print("---DISPLAYING AGE OF EACH EQUPMENT INVENTORY UNIT---\n")
    for data in equipment_units_data:
        print(f"Unit ID: {data[0]}")
        print(f"Type: {data[1]}")
        print("Purchase Date: {}".format(data[2].strftime('%b. %Y')))
        print(f"Is > 5 years old: {is_older_than_five(data[2], date.today())}\n\n")


    # ----
    # Note: None of our equipment units are older than 5 years. This makes the report less interesting
    # ----

# Report to summarize the details for each planned excursion
# Use this information to send a welcome message and trip summary to customer upon registration
def excursion_summary_report(my_cursor):
    def get_query_result(query):
        my_cursor.execute(query)
        return my_cursor.fetchall()

    # Big query to gather all relevent data for an excursion
    excursion_summary_query = """
    select excursions.id, trip_type.tripName, excursions.excursionDate, excursions.visaRequired, excursions.aireFarePerPerson, equipment.equipmentName, vaccinations.vaccinationName, customers.lastName
    from excursions
    left join trip_type on excursions.tripTypeId = trip_type.id
    left join customers on excursions.id = customers.excursionId
    left join equipment_trip on trip_type.id = equipment_trip.tripId
    left join equipment on equipment.id = equipment_trip.equipmentId
    left join required_trip_vaccinations on trip_type.id = required_trip_vaccinations.tripId
    left join vaccinations on vaccinations.id = required_trip_vaccinations.vaccinationId;"""

    # Retrieve data from MySql using query
    excursion_summary_data = get_query_result(excursion_summary_query)

    # The first element in each returned tuple from the MySql query is the unique excursion id, lets collect those
    def get_unique_excursions(summary_data):
        excursion_ids = []
        for data in summary_data:
            excursion_ids.append(data[0])

        # Get distinct excursion ids
        unique_excursion_ids = list(set(excursion_ids))
        return unique_excursion_ids

    # Prepare a dictionary object so that we may map our MySql data into a more useful form
    def create_summary_dictionary(excursions):
        summary_dictionary = {}
        for excursion in excursions:
            summary_dictionary[excursion] = {
                "tripType": "",
                "date": "",
                "visaRequired": "",
                "aireFarePerPerson": "",
                "equipment": [],
                "vaccinations": [],
                "customers": []
            }
        return summary_dictionary

    # Use this to transform the MySql booleans from zeros and ones to Trues and Falses
    def display_bool(myBool):
        if myBool == 0:
            return False
        else:
            return True

    # Map the returned MySql data into a useful data object/dictionary
    def map_summary_data(summary_data):
        unique_excursions = get_unique_excursions(summary_data)
        summary_dictionary = create_summary_dictionary(unique_excursions)

        # Map data into summary_dictionary
        for data in summary_data:
            if summary_dictionary[data[0]]["tripType"] == "":
                summary_dictionary[data[0]]["tripType"] = data[1]

            if summary_dictionary[data[0]]["date"] == "":
                summary_dictionary[data[0]]["date"] = data[2].strftime("%b. %d %Y")
       
            if summary_dictionary[data[0]]["visaRequired"] == "":
                summary_dictionary[data[0]]["visaRequired"] = display_bool(data[3])

            if summary_dictionary[data[0]]["aireFarePerPerson"] == "":
                summary_dictionary[data[0]]["aireFarePerPerson"] = str(data[4])

            if data[5] not in summary_dictionary[data[0]]["equipment"] and data[5] != None:
                summary_dictionary[data[0]]["equipment"].append(data[5])

            if data[6] not in summary_dictionary[data[0]]["vaccinations"] and data[6] != None:
                summary_dictionary[data[0]]["vaccinations"].append(data[6])

            if data[7] not in summary_dictionary[data[0]]["customers"] and data[7] != None:
                summary_dictionary[data[0]]["customers"].append(data[7])

        return summary_dictionary

    excursion_summary_dictionary = map_summary_data(excursion_summary_data)

    # Iterate through the summary dictionary to to display excursion data
    print("--DISPLAYING EXCURSION SUMMARIES--\n")
    for excursion in excursion_summary_dictionary:
        print("Trip: {}".format(excursion_summary_dictionary[excursion]["tripType"]))
        print("Date: {}".format(excursion_summary_dictionary[excursion]["date"]))
        print("Visa Required: {}".format(excursion_summary_dictionary[excursion]["visaRequired"]))
        print("Aire Fare: ${}".format(excursion_summary_dictionary[excursion]["aireFarePerPerson"]))
        print("-Equipment Required-")
        for equipment in excursion_summary_dictionary[excursion]["equipment"]:
            print(f"{equipment}")
        print("-Vaccinatons Required-")
        for vacc in excursion_summary_dictionary[excursion]["vaccinations"]:
            print(f"{vacc}")
        print("-Customers Participating-")
        for customer in excursion_summary_dictionary[excursion]["customers"]:
            print(f"{customer}")
        print("\n\n")

# Function to display all reports for this assignment
def display_reports(config):

    try: 
        db = mysql.connector.connect(**config)
        print("\nDatabase user {} connected to MySql on host {} with database {}\n\n\n".format(config["user"], config["host"], config["database"]))
        my_cursor = db.cursor()

        display_remaining_vaccinatons_needed_for_each_customer(my_cursor)
        display_equipment_age_report(my_cursor)
        excursion_summary_report(my_cursor)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("   The supplied username or password are invalid")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("   The specified database does not exist")
        else:
            print(err)
    finally:
        db.close()

def main():
    config = {
    "user": "root",
    "password": input("Please enter your root database password: "), #YOUR MYSQL PASSWORD HERE!
    "host": "127.0.0.1",
    "database": "OutlandAdventuresCase",
    "raise_on_warnings": True
    }

    # Display reports for this assignment
    display_reports(config)

    input("\nPress ENTER to exit...")

if __name__ == '__main__':
    main()