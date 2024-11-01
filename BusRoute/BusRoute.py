import cmd
import mysql.connector
from mysql.connector import Error
import csv
import os

class DatabaseCLI(cmd.Cmd):
    intro = 'Welcome to Bus Routes Database CLI! Type "help" to list commands. To run your test file, enter the command "run".'
    prompt = 'Database CLI: '

    # Constructor, holds csv file and generates database if needed
    def __init__(self, csv_file):
        super().__init__()
        self.csv_file = 'testcase/' + csv_file
        self.connection = self.create_connection()
        self.create_database("dbprog")  

    
    
    # Base function to connect to database
    def create_connection(self):
        try:
            connection = mysql.connector.connect(
                host="localhost",
                user="cs5330",
                password="pw5330"  
            )
            if connection.is_connected():
                print("Connecting to the Bus Routes server...")
                return connection
        except Error as e:
            print(f"Error: {e}")
            return None

    
    # If database does not exist, create it. Catches test case of no database
    def create_database(self, db_name):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute("USE dbprog")
        except Error as e:
            print(f"Error: {e}")
        finally:
            cursor.close()
    
    
    
    # Function to generate tables, used in functions below 
    def generate(self):
        """Generates missing tables: Terminal, Route, and LeaveTime."""
        
        try:
            with self.connection.cursor() as cursor:
            
                cursor.execute("USE dbprog")
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'dbprog' 
                    AND table_name IN ('Terminal', 'Route', 'LeaveTime')
                """)
                existing_tables = {row[0] for row in cursor.fetchall()}

                # Create Terminal table if it doesn't exist
                if 'Terminal' not in existing_tables:
                    cursor.execute("""
                        CREATE TABLE Terminal (
                            Name VARCHAR(255) PRIMARY KEY, 
                            District VARCHAR(255)
                        )
                    """)
                    print("Table 'Terminal' created.")

                # Create Route table if it doesn't exist
                if 'Route' not in existing_tables:
                    cursor.execute("""
                        CREATE TABLE Route (
                            RouteNum INT PRIMARY KEY, 
                            Source VARCHAR(255),
                            Destination VARCHAR(255),
                            TravelTime INT CHECK (TravelTime > 0), 
                            Fare DECIMAL(5,2) CHECK (Fare > 0),
                            FOREIGN KEY (Source) REFERENCES Terminal(Name),
                            FOREIGN KEY (Destination) REFERENCES Terminal(Name)
                        )
                    """)
                    print("Table 'Route' created.")

                # Create LeaveTime table if it doesn't exist
                if 'LeaveTime' not in existing_tables:
                    cursor.execute("""
                        CREATE TABLE LeaveTime (
                            RouteNum INT, 
                            LeaveTime TIME,
                            PRIMARY KEY(RouteNum, LeaveTime),
                            FOREIGN KEY (RouteNum) REFERENCES Route(RouteNum)
                        )
                    """)
                    print("Table 'LeaveTime' created.")

            self.connection.commit()

        except Exception as e:
            print(f"An error occurred during table creation: {e}")

    
    
    
    # Function to check for tables
    def do_e(self, arg):
        'Checks if Tables Exist (If tables do not exist, generates tables): e'
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("USE dbprog")
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'dbprog' 
                AND table_name IN ('Terminal', 'Route', 'LeaveTime')
            """)
            existing_tables = {row[0] for row in cursor.fetchall()}

        # If all tables exist, state they do. If not go to error block to generate missing tables.
            if {'Terminal', 'Route', 'LeaveTime'}.issubset(existing_tables):
                return
            else:
                self.generate()

        finally:
            cursor.close()
    
    
    
    # Function to clear all data from tables 
    def do_r(self, arg):
        'Clears all data from tables (If tables do not exist, generates tables): r'

        cursor = self.connection.cursor()
        try:
            cursor.execute("USE dbprog")

            # Clear data with DELETE, which respects foreign key constraints
            cursor.execute("DELETE FROM LeaveTime;")
            cursor.execute("DELETE FROM Route;")
            cursor.execute("DELETE FROM Terminal;")
            
            self.connection.commit()
            print('Data from tables deleted')

        except Error as e:
            print(f"Error: {e}")

        finally:
            cursor.close()

    

    
    # Function to add a new terminal to database 
    def do_t(self, arg):
        'Adds a new terminal: t <name of terminal>  <district>'
        
        cursor = self.connection.cursor()
        try:
            arguments = arg.split()

            if len(arguments)  != 2:
                print("Invalid Input: Right amount of arguments were not passed for command ")
                return
            
            name = arguments[0]
            district = arguments[1]
            cursor.execute("USE dbprog")
            cursor.execute("""
                INSERT INTO Terminal (Name, District)
                VALUES (%s, %s)""", (name, district))
            self.connection.commit()
        
        except Exception as e:
            self.connection.rollback()
            print(f"t, {name}, {district} Input Invalid")
        
        finally:
            cursor.close()


    # Function to list information about a terminal 
    def do_T(self, arg):
        'Lists information about terminal: T <terminal name>'

        cursor = self.connection.cursor()

        try:
            cursor.execute("USE dbprog")
            arguments = arg.split()

            if len(arguments) != 1:
                print(f"T, {terminal} Invalid Input")
                return

            # Get the terminal name from arguments
            terminal = arguments[0]

            # Query to select terminal information
            cursor.execute("""
                SELECT Name, District
                FROM Terminal
                WHERE Name = %s
            """, (terminal,))
            terminal_info = cursor.fetchone()

            if not terminal_info:
                print(f"T, {terminal} Invalid Input")
                return

            # Query for routes with this terminal as the source
            cursor.execute("""
                SELECT RouteNum
                FROM Route
                WHERE Source = %s
                ORDER BY RouteNum ASC
            """, (terminal,))
            source_routes = cursor.fetchall()
            
            # Query for routes with this terminal as the destination
            cursor.execute("""
                SELECT RouteNum
                FROM Route
                WHERE Destination = %s
                ORDER BY RouteNum ASC
            """, (terminal,))
            destination_routes = cursor.fetchall()

            # Prepare the output
            terminal_name, district = terminal_info
            source_count = len(source_routes)
            destination_count = len(destination_routes)

            # Print the output in the specified format
            print(f'{terminal_name} {district}')
            print(f'{source_count} ' + ", ".join(str(route[0]) for route in source_routes))
            print(f"{destination_count} " + ", ".join(str(route[0]) for route in destination_routes))

        except Exception as e:
            print(f"T, {terminal} Invalid Input")

        finally:
            cursor.close()


    def do_l(self, arg):
        """Records the departure time of a bus for a specific route, ensuring no conflicts in schedule and valid times."""

        arguments = arg.split()
        
        if len(arguments) != 2:
            print(f"l, {route_number}, {start_time_str} Invalid Input")
            return

        # Extract and validate route number and start time
        try:
            
            # Go through test cases first
            route_number = int(arguments[0])
            if route_number <= 0:
                print(f"l, {route_number}, {start_time_str} Invalid Input")
                return
        except ValueError:
            print(f"l, {route_number}, {start_time_str} Invalid Input")
            return

        start_time_str = arguments[1]
        if len(start_time_str) != 4 or not start_time_str.isdigit():
            print(f"l, {route_number}, {start_time_str} Invalid Input")
            return

        # Convert hours and minutes, validate time range (test case 3100)
        hours, minutes = int(start_time_str[:2]), int(start_time_str[2:])
        if hours < 5 or (hours == 23 and minutes > 0) or hours > 23 or minutes < 0 or minutes > 59:
            print(f"l, {route_number}, {start_time_str} Invalid Input")
            return

        # Convert start time to hh:mm format for MySQL
        start_time = f"{start_time_str[:2]}:{start_time_str[2:]}:00"

        try:
            with self.connection.cursor() as cursor:
                
                cursor.execute("USE dbprog")

                # Check for conflicts with other buses on the same route
                cursor.execute("""
                    SELECT LeaveTime
                    FROM LeaveTime
                    WHERE RouteNum = %s AND (
                        LeaveTime >= SUBTIME(%s, '00:14:00') AND LeaveTime <= ADDTIME(%s, '00:14:00')
                    )
                """, (route_number, start_time, start_time))

                conflicts_on_route = cursor.fetchall()

                # Check for conflicts with all buses (different routes)
                cursor.execute("""
                    SELECT LeaveTime
                    FROM LeaveTime
                    WHERE LeaveTime = %s
                """, (start_time,))

                conflicts_at_same_time = cursor.fetchall()

                if conflicts_on_route or conflicts_at_same_time:
                    print(f"l, {route_number}, {start_time_str} Invalid Input")
                    return

                # Insert the new departure time for the specified route
                cursor.execute("""
                    INSERT INTO LeaveTime (RouteNum, LeaveTime)
                    VALUES (%s, %s)
                """, (route_number, start_time))

                self.connection.commit()

        except Exception as e:
            print(f"l, {route_number}, {start_time_str} Invalid Input")



    # Function to return information about a route 
    def do_B(self, arg):
        'Returns information about a certain Route: B <route number>'

        cursor = self.connection.cursor()

        try:
            
            cursor.execute("USE dbprog")
            route_num = arg.strip()

            # Ensure a route number is provided
            if not route_num:
                print("Invalid Input: The command requires exactly one argument: <route number>")
                return

            # Fetch route information: route number, source, destination, travel time, and fare
            cursor.execute("""
                SELECT RouteNum, Source, Destination, TravelTime, Fare
                FROM Route
                WHERE RouteNum = %s
            """, (route_num,))
            route_info = cursor.fetchone()

            if route_info:
                route_number, source, destination, travel_time, fare = route_info
                # Print route information
                print(f"{route_number} {source} {destination} "
                    f"{travel_time} {fare:.2f}")
            else:
                print(f"Route '{route_num}' not found in the database.")
                return

            # Fetch leave times for the route in ascending order
            cursor.execute("""
                SELECT LeaveTime
                FROM LeaveTime
                WHERE RouteNum = %s
                ORDER BY LeaveTime ASC
            """, (route_num,))
            leave_times = cursor.fetchall()

            
            leave_times_str = " ".join(str(time[0]).split(":")[0] + ":" + str(time[0]).split(":")[1] for time in leave_times)
            if leave_times_str:
                print(leave_times_str)


        except Exception as e:
            print(f"Error: {e}")

        finally:
            cursor.close()



    # Function that returns all districts and the routes they use
    def do_D(self, arg):
        'Returns information about all districts and routes that use it: D'

        cursor = self.connection.cursor()
        
        try:
            cursor.execute("USE dbprog")
            cursor.execute("""
                SELECT DISTINCT District
                FROM Terminal
            """)
            districts = cursor.fetchall()

            # Counting for each district
            for district_row in districts:
                district = district_row[0]

                
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM Route
                    WHERE Source IN (SELECT Name FROM Terminal WHERE District = %s)
                """, (district,))
                source_count = cursor.fetchone()[0]
                
                
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM Route
                    WHERE Destination IN (SELECT Name FROM Terminal WHERE District = %s)
                """, (district,))
                destination_count = cursor.fetchone()[0]

                # Display the result for the district
                print(f"{district} {source_count} {destination_count}")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            cursor.close()

    
        
    # Function to find  bus routes from source to destination in within one transfer (NEEDS WORK)
    def do_C(self, arg):
        'Finds bus routes from source to destination within one bus change: C <source terminal> <destination terminal>'

        cursor = self.connection.cursor()

        try:
            cursor.execute("USE dbprog")
            arguments = arg.split()

            if len(arguments) != 2:
                print("Invalid Input: The command requires exactly two arguments: <source terminal> <destination terminal>")
                return

            source, destination = arguments

            # Finds the direct routes from source to destination
            cursor.execute("""
                SELECT RouteNum, Fare
                FROM Route
                WHERE Source = %s AND Destination = %s
            """, (source, destination))
            direct_routes = cursor.fetchall()

            for route_num, fare in direct_routes:
                print(f"{route_num} {fare:.2f}")

            # Finds one-transfer routes
            cursor.execute("""
                SELECT R1.RouteNum, R1.Destination, R1.Fare, R2.RouteNum, R2.Fare
                FROM Route R1
                JOIN Route R2 ON R1.Destination = R2.Source
                WHERE R1.Source = %s AND R2.Destination = %s
            """, (source, destination))
            transfer_routes = cursor.fetchall()

            for route_num1, intermediate, fare1, route_num2, fare2 in transfer_routes:
                total_fare = fare1 + fare2
                print(f"{route_num1} {route_num2} {total_fare:.2f}")

            # Case for if no routes found
            if not direct_routes and not transfer_routes:
                print("None")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            cursor.close()

    

    # Function to find bus routes from source to destination given a starting time in two transfers 
    def do_F(self, arg):
        'Finds bus routes from source to destination within two bus changes given a starting time: F <source terminal> <destination terminal> <time>'
        
        results = []  

        try:
            cursor = self.connection.cursor()
            cursor.execute("USE dbprog")
            arguments = arg.split()

            if len(arguments) != 3:
                print("Invalid Input: The command requires exactly three arguments: <source terminal> <destination terminal> <time>")
                return

            source, destination, start_time_str = arguments

            # Validate start time
            if len(start_time_str) != 4 or not start_time_str.isdigit():
                print("Invalid Input: Start time must be in hhmm format (e.g., 0730 for 7:30 am).")
                return

            # Convert start time to total minutes
            start_hours = int(start_time_str[:2])
            start_minutes = int(start_time_str[2:])
            start_total_minutes = start_hours * 60 + start_minutes
            end_total_minutes = start_total_minutes + 60  # End time is 1 hour after start time

            # Find direct routes from source to destination
            cursor.execute("""
                SELECT R.RouteNum, R.Fare, R.TravelTime, 
                    (HOUR(L.LeaveTime) * 60 + MINUTE(L.LeaveTime)) AS LeaveTimeMinutes
                FROM Route R
                JOIN LeaveTime L ON R.RouteNum = L.RouteNum
                WHERE R.Source = %s AND R.Destination = %s
                AND (HOUR(L.LeaveTime) * 60 + MINUTE(L.LeaveTime)) >= %s
                AND (HOUR(L.LeaveTime) * 60 + MINUTE(L.LeaveTime)) <= %s
                ORDER BY LeaveTimeMinutes ASC
            """, (source, destination, start_total_minutes, end_total_minutes))

            direct_routes = cursor.fetchall()

            if direct_routes:
                for route_num, fare, travel_time, leave_time_minutes in direct_routes:
                    arrival_time_minutes = leave_time_minutes + travel_time
                    total_travel_time = arrival_time_minutes - start_total_minutes
                    results.append((route_num, fare, total_travel_time))  # Store the result

            # Find one-transfer routes
            cursor.execute("""
                SELECT R1.RouteNum AS first_route, R1.Fare AS fare1, R1.TravelTime AS travel_time1, 
                    (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) AS leave_time1,
                    R2.RouteNum AS second_route, R2.Fare AS fare2, R2.TravelTime AS travel_time2, 
                    (HOUR(L2.LeaveTime) * 60 + MINUTE(L2.LeaveTime)) AS leave_time2
                FROM Route R1
                JOIN LeaveTime L1 ON R1.RouteNum = L1.RouteNum
                JOIN Route R2 ON R1.Destination = R2.Source
                JOIN LeaveTime L2 ON R2.RouteNum = L2.RouteNum
                WHERE R1.Source = %s AND R2.Destination = %s
                AND (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) >= %s
                AND (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) <= %s
                AND (HOUR(L2.LeaveTime) * 60 + MINUTE(L2.LeaveTime)) >= (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) + 5
                ORDER BY leave_time1, leave_time2 ASC
            """, (source, destination, start_total_minutes, end_total_minutes))

            one_transfer_routes = cursor.fetchall()

            if one_transfer_routes:
                for first_route, fare1, travel_time1, leave_time1, second_route, fare2, travel_time2, leave_time2 in one_transfer_routes:
                    total_fare = fare1 + fare2
                    arrival_time_first_leg = leave_time1 + travel_time1
                    total_travel_time = (leave_time2 - arrival_time_first_leg + 20) + travel_time2  # Including travel time and waiting
                    results.append((first_route, second_route, total_fare, total_travel_time))  # Store the result

            # Find two-transfer routes
            cursor.execute("""
                SELECT R1.RouteNum AS first_route, R1.Fare AS fare1, R1.TravelTime AS travel_time1, 
                    (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) AS leave_time1,
                    R2.RouteNum AS second_route, R2.Fare AS fare2, R2.TravelTime AS travel_time2, 
                    (HOUR(L2.LeaveTime) * 60 + MINUTE(L2.LeaveTime)) AS leave_time2,
                    R3.RouteNum AS third_route, R3.Fare AS fare3, R3.TravelTime AS travel_time3, 
                    (HOUR(L3.LeaveTime) * 60 + MINUTE(L3.LeaveTime)) AS leave_time3
                FROM Route R1
                JOIN LeaveTime L1 ON R1.RouteNum = L1.RouteNum
                JOIN Route R2 ON R1.Destination = R2.Source
                JOIN LeaveTime L2 ON R2.RouteNum = L2.RouteNum
                JOIN Route R3 ON R2.Destination = R3.Source
                JOIN LeaveTime L3 ON R3.RouteNum = L3.RouteNum
                WHERE R1.Source = %s AND R3.Destination = %s
                AND (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) >= %s
                AND (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) <= %s
                AND (HOUR(L2.LeaveTime) * 60 + MINUTE(L2.LeaveTime)) >= (HOUR(L1.LeaveTime) * 60 + MINUTE(L1.LeaveTime)) + 5
                AND (HOUR(L3.LeaveTime) * 60 + MINUTE(L3.LeaveTime)) >= (HOUR(L2.LeaveTime) * 60 + MINUTE(L2.LeaveTime)) + 5
                ORDER BY leave_time1, leave_time2, leave_time3 ASC
            """, (source, destination, start_total_minutes, end_total_minutes))

            two_transfer_routes = cursor.fetchall()

            if two_transfer_routes:
                for first_route, fare1, travel_time1, leave_time1, second_route, fare2, travel_time2, leave_time2, third_route, fare3, travel_time3, leave_time3 in two_transfer_routes:
                    total_fare = fare1 + fare2 + fare3
                    arrival_time_second_leg = leave_time2 + travel_time2
                    total_travel_time = (arrival_time_second_leg - start_total_minutes + 20) + travel_time3  
                    results.append((first_route, second_route, third_route, total_fare, total_travel_time))  
                
            results.sort(key=lambda x: (x[-1], x[0] if len(x) == 3 else x[1] if len(x) == 4 else x[2]))

            for result in results:
                print(" ".join(map(str, result)))

        except Exception as e:
            print(f"Error: {e}")

        finally:
            cursor.close()



    # Function that enters information about the bus route
    def do_b(self, arg):
        'Enters information about a bus route: b <route number> <source terminal> <destination terminal> <travel time> <fare>'

        cursor = self.connection.cursor()
        try:
            cursor.execute("USE dbprog")
            arguments = arg.split()

            # Check if we have exactly five arguments
            if len(arguments) != 5:
                print("Error: The command requires exactly five arguments: <route number> <source terminal> <destination terminal> <travel time> <fare>")
                return

            # Grab the info
            route_num = int(arguments[0])
            source_terminal = arguments[1]
            destination_terminal = arguments[2]
            travel_time = int(arguments[3])
            fare = float(arguments[4])

            # Insert the route into the table
            cursor.execute("""
                INSERT INTO Route (RouteNum, Source, Destination, TravelTime, Fare)
                VALUES (%s, %s, %s, %s, %s)
            """, (route_num, source_terminal, destination_terminal, travel_time, fare))
        
            self.connection.commit()
        
        except Exception as e:
            self.connection.rollback()
            print(f"b, {route_num}, {source_terminal}, {destination_terminal}, {travel_time}, {fare} Invalid Input")

        finally:
            cursor.close()
    

  
    # Function to run file commands
    def do_run(self, arg):
        'Runs selected command file: run'
        
        try:
            if not os.path.exists(self.csv_file):
                print(f'File "{self.csv_file}" does not exist. Run command "test" to input new testcase')
                return

            with open(self.csv_file, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if not row:
                        continue
                    
                    # Process command and arguments from each row
                    command = row[0].strip()
                    args = " ".join(part.strip() for part in row[1:])
                    full_command = f"{command} {args}"
                    self.onecmd(full_command)
        
        except Exception as e:
            print(f"Error running commands from file: {e}")



    # Function to reset testfile to be ran
    def do_test(self, arg):
        'Inputs a new test file to be ran: testfile'
        
        data_input = str(input("Please enter the name of the file you would like to use (include .csv): "))
        filepath = 'testcase/' + data_input
        self.csv_file = filepath
        print("Your new test file to be used is: " + data_input)
    
    
    
    # Function to exit the terminal
    def do_exit(self, arg):
        'Exits the CLI: exit'
        
        print("Exiting...")
        self.connection.close()
        return True


if __name__ == '__main__':
    data_input = str(input("Please enter the name of the file you would like to use (include .csv): "))
    csv_file = data_input
    print("Your test file to be used is: " + csv_file)
    
    DatabaseCLI(csv_file).cmdloop()
