import mysql.connector
from mysql.connector import errorcode

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
    
    # Final dictionary of customers and their corresponding list of needed vaccinatons that they don't have yet
    needed_customer_vacc_dict = generate_needed_customer_vacc_dict(customer_vacc_dict, required_customer_vacc_dict)

    # Function to display each customer and their remaining needed vaccinatons. This is the report.
    def display_needed_customer_vaccs(needed_customer_vacc_dict):
        print("---DISPLAYING REMAINING NEEDED VACCINATONS TO FULFILL TRIP REQUIREMENTS---\n")
        for customer in needed_customer_vacc_dict:
            print(f"--Customer with last name of {customer} still needs the following vaccinatons:")
            for vacc in needed_customer_vacc_dict[customer]:
                print(f"the {vacc.upper()} vaccinaton\n")

    # Use the display function to display report
    display_needed_customer_vaccs(needed_customer_vacc_dict)
# ---------
# Note: in the data, there is no customer who is going on two excursions, this makes this report less interesting
# Note: three out of five of the customers in the data are going to Fjords of Norway which doesn't require any vaccinations, this makes the report less interesting
# Consider: Changing the data to make the report more interesting?
#----------

def excursion_details(my_cursor):
    def get_query_result(query):
        my_cursor.execute(query)
        return my_cursor.fetchall()
    
    # Excursions Details Report
    # data needed --> (Excursion name, customers attending, equipment needed, vaccinations required)
    # Group data to display organized by Excursion

    # Queries to collect data
    # Excursion name and date
    excursion_name_query = """
    SELECT trip_type.trip_name, excursion.excursionDate
    FROM excursions
    LEFT JOIN trip_type ON trip_type.id = excursions.tripTypeId"""

    # Customers attending by Last Name
    customers_attending_query = """
    SELECT trip_type.trip_name, customers.lastName
    FROM excursions
    LEFT JOIN customers ON excursions.id = customers.excursionId
    LEFT JOIN trip_type ON trip_type.id = excursions.tripTypeId"""

    # Equipment needed
    equipment_needed_query = """
    SELECT trip_type.trip_name, equipment.equipmentName
    FROM equipment
    LEFT JOIN equipment_trip ON equipment.id = equipment_trip.equipmentId
    LEFT JOIN trip_type ON trip_type.id = equipment_trip.tripId"""

    # Vaccinations required
    vaccs_per_trip_query = """
    SELECT trip_type.tripName, vaccinations.vaccinationName
    FROM trip_type
    LEFT JOIN required_trip_vaccinations ON trip_type.id = required_trip_vaccinations.tripId
    LEFT JOIN vaccinations ON required_trip_vaccinations.vaccinationId = vaccinations.id"""

    excursion_name_data = get_query_result(excursion_name_query)
    customers_attending_data = get_query_result(customers_attending_query)
    equipment_needed_data = get_query_result(equipment_needed_query)
    vaccs_per_trip_data = get_query_result(vaccs_per_trip_query)
    

# Function to display all reports for this assignment
def display_reports(config):

    try: 
        db = mysql.connector.connect(**config)
        print("\nDatabase user {} connected to MySql on host {} with database {}\n\n\n".format(config["user"], config["host"], config["database"]))
        my_cursor = db.cursor()

        # Display report regarding the remaining vaccinations customers need to go on their excurions
        display_remaining_vaccinatons_needed_for_each_customer(my_cursor)

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