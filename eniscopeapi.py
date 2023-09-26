import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.fernet import Fernet
import credentials


class EniscopeAPIClient:
    def __init__(self, api_key, base_url="https://core.eniscope.com/v1/"):
        """
        Initialize the Eniscope API Client.

        Parameters:
        - api_key (str): Your Eniscope API key.
        - base_url (str, optional): The base URL of the Eniscope API. Default is the production URL.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.encoded_auth = None
        self.encryption_key = credentials.encryption_key
        self.headers = None
        self.session = requests.Session()
        self.session.headers.update({"X-Eniscope-API": api_key, "Accept": "text/json"})
        self.response = None

    def authenticate_user(self):
        """
        Authenticate the user with the Eniscope API using stored or provided credentials.

        Returns:
        - bool: True if authentication is successful, False otherwise.
        """
        try:
            with open("eniscope_api.conf", "r") as config_file:
                config = json.load(config_file)
                encrypted_credentials = config.get("credentials").encode()
        except FileNotFoundError:
            print("Credentials file not found. Please run the setup script.")
            return False

        decoded_credentials = self.decrypt(encrypted_credentials)

        self.headers = {
            "Authorization": f"Basic {decoded_credentials}",
            "Accept": "text/json",  # Default response content type
        }

        response = self.session.get(self.base_url, headers=self.headers)
        if response.status_code == 200:
            self.headers = {
                "X-Eniscope-API": self.api_key,
                "X-Eniscope-Token": response.headers["X-Eniscope-Token"],
            }
            self.session.headers.update(self.headers)
            return True
        else:
            return False

    def decrypt(self, encrypted_data):
        """
        Decrypt and decode encrypted data using the Fernet encryption key.

        Parameters:
        - encrypted_data (bytes): The encrypted data to decrypt.

        Returns:
        - str: The decrypted and decoded data as a string.
        """
        cipher_suite = Fernet(self.encryption_key)
        decrypted_data = cipher_suite.decrypt(encrypted_data)
        return decrypted_data.decode()

    def get_request_data(self, url):
        """
        Send a GET request and return the JSON response data.

        Parameters:
        - url (str): The URL to send the GET request to.

        Returns:
        - dict: The JSON response data.
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return json.loads(response.text)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None

    def options_request(self, url):
        """
        Send an OPTIONS request and return the JSON response data.

        Parameters:
        - url (str): The URL to send the OPTIONS request to.

        Returns:
        - dict: The JSON response data.
        """
        try:
            response = self.session.options(url)
            response.raise_for_status()
            return json.loads(response.text)
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None

    def get_user_details(self):
        """
        Retrieve details about the logged-in user.

        Returns:
        - dict: User details.
        """
        url = f"{self.base_url}"
        response = self.get_request_data(url)
        return response

    def get_organizations_list(self, organization_id=None, organization_name=None):
        """
        Retrieve a list of organizations viewable by the logged-in user.

        Parameters:
        - organization_id (str, optional): The ID of a specific organization to retrieve. Default is None.
        - organization_name (str, optional): The name of a specific organization to retrieve. Default is None.
        Organization_id is preferred over organization_name if both are provided.

        Returns:
        - list: List of  dictionaries of organizations.
        """
        if organization_id:
            url = f"{self.base_url}organizations/?id={organization_id}"
        elif organization_name:
            url = f"{self.base_url}organizations/?name={(organization_name).replace(' ','%20')}"

        else:
            url = f"{self.base_url}organizations/"
        response = self.get_request_data(url)
        return response["organizations"]

    def get_channels_list(self, organization_id):
        """
        Retrieve a list of channels for a specific organization.

        Parameters:
        - organization_id (str or list): The IDx of the organization to retrieve channels for.

        Returns:
        - dict: List of channels.
        """

        if isinstance(organization_id, list):
            max_workers = len(organization_id)
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:

                def query_single(organization_id):
                    url = f"{self.base_url}channels/?organization={organization_id}&limit=0"
                    try:
                        response = self.get_request_data(url)
                        results.append(response["channels"])

                    except Exception as e:
                        results.append({"error": str(e)})

                    finally:
                        # Print a dot to the console to indicate progress
                        print(".", end="", flush=True)

                for org_id in organization_id:
                    executor.submit(query_single, org_id)
                executor.shutdown(wait=True)
            return results

        else:
            url = f"{self.base_url}channels/?organization={organization_id}&limit=0"
            response = self.get_request_data(url)
            return response["channels"]

    def get_channel_data(
        self, channel_id, start_date, end_date, fields=None, resolution=60
    ):
        """
        Retrieve channel data for a specified channel ID and date range.

        Parameters:
        - channel_id (str): The ID of the channel to retrieve data for.
        - start_date (int): The start date of the data range.
        - end_date (int): The end date of the data range.
        - fields (list, optional): List of fields to retrieve. Default is None.
        - resolution (int, optional): The resolution of the data. Default is 60.

        Returns:
        - dict: Channel data.
        """
        if not fields:
            url = f"{self.base_url}/readings/{channel_id}/"
            response = self.options_request(url)
            self.__shape_fields__(response["filters"]["fields"])
        else:
            self.__shape_fields__(fields)
        url = f"{self.base_url}readings/{channel_id}/?action=summarise&{self.fields}daterange[]={start_date}&daterange[]={end_date}&res={resolution}"

        response = self.get_request_data(url)
        return response

    def __shape_fields__(self, fields: list):
        """
        Helper function to construct the 'fields' parameter for the URL.

        Parameters:
        - fields (list): List of fields to include in the URL.
        """
        self.fields = ""
        for field in fields:
            self.fields += f"fields[]={field}&"

    def get_multiple_channel_data(
        self, channel_ids, date_ranges, fields=None, resolution=60
    ):
        """
        Retrieve data for multiple channels and date ranges simultaneously.

        Parameters:
        - channel_ids (list): List of channel IDs to retrieve data for.
        - date_ranges (list): List of date ranges in the format [(start_date, end_date)].
        - fields (list, optional): List of fields to retrieve. Default is None.
        - resolution (int, optional): The resolution of the data. Default is 60.

        Returns:
        - dict: Dictionary of channel data with keys in the format 'channel_id_start_date_end_date'.
        """
        data = {}
        THREAD_LIMIT = 20

        # Create a generator of all tasks
        tasks = (
            (channel_id, date_range)
            for channel_id in channel_ids
            for date_range in date_ranges
        )

        def query_single(channel_id, date_range):
            start_date, end_date = date_range
            try:
                result = self.get_channel_data(
                    channel_id, start_date, end_date, fields, resolution
                )
                return channel_id, start_date, end_date, result
            except Exception as e:
                print(f"\nError for {channel_id} {start_date} {end_date}: {str(e)}")

        # Determine the maximum number of workers based on the input lists
        max_workers = min(THREAD_LIMIT, len(channel_ids) * len(date_ranges))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(query_single, channel_id, date_range): (
                    channel_id,
                    date_range,
                )
                for channel_id, date_range in tasks
            }

            for future in as_completed(futures):
                channel_id, start_date, end_date, result = future.result()
                data[f"{channel_id}_{start_date}_{end_date}"] = result
                # Print a dot to the console to indicate progress
                print(".", end="", flush=True)

                # Submit new tasks as threads become available, if any are left
                try:
                    next_task = next(tasks)
                    futures[executor.submit(query_single, *next_task)] = next_task
                except StopIteration:
                    # No more tasks left
                    pass
        return data

    def get_alarm_data(self, organization_id):
        """
        Retrieve alarm data for a specified organization ID with respective alarm rules and periods.

        Parameters:
        - organization_id (str or list): The ID of the organization to retrieve data for.


        Returns:
        - dict: Alarm data.
        - dict: Alarm rules for each alarmId.
        - dict: Alarm periods for each alarmId.
        """
        alarms_dict = []

        # in case of multiple organizations

        if isinstance(organization_id, list):
            max_workers = len(organization_id)
            org_alarms_dict = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:

                def query_single(organization_id):
                    url = (
                        f"{self.base_url}alarms/?organization={organization_id}&limit=0"
                    )
                    try:
                        response = self.get_request_data(url)
                        org_alarms_dict.append(response["alarms"])
                    except Exception as e:
                        org_alarms_dict.append({"error": str(e)})
                    finally:
                        # Print a dot to the console to indicate progress
                        print(".", end="", flush=True)

                for org_id in organization_id:
                    executor.submit(query_single, org_id)
                executor.shutdown(wait=True)

                # collect list of alarms for different organizations in a single alarms dictionary

                for org_alarms in org_alarms_dict:
                    for alarm in org_alarms:
                        alarms_dict.append(alarm)
        # if only one organization
        else:
            url = f"{self.base_url}alarms/?organization={organization_id}&limit=0"

            response = self.get_request_data(url)
            alarms_dict = response["alarms"]

        # once list of alarms for one or multiple arganisation is ready, get alarm rules and periods
        # create list of alarm ids
        alarms_id_list = []
        for alarms in alarms_dict:
            alarms_id_list.append(alarms["alarmId"])

        # empty lists for alarm rules and periods
        alarm_rules_dict = []
        alarm_periods_dict = []

        with ThreadPoolExecutor(max_workers=len(alarms_id_list * 2)) as executor:

            def query_single_rules(alarm_id):
                try:
                    result = self.get_alarm_rules(alarm_id)
                    alarm_rules_dict.append(result)
                except Exception as e:
                    print(
                        f"\nError while retrieving alarm rules for {alarm_id}. Alarm will be skipped. Check alarm settings at https://analytics.eniscope.com/alarm/edit/{alarm_id}"
                    )
                finally:
                    # Print a dot to the console to indicate progress
                    print(".", end="", flush=True)

            def query_single_periods(alarm_id):
                try:
                    result = self.get_alarm_periods(alarm_id)
                    alarm_periods_dict.append(result)
                except Exception as e:
                    print(
                        f"\nError while retrieving alarm periods for {alarm_id}. Alarm will be skipped. Check alarm settings at https://analytics.eniscope.com/alarm/edit/{alarm_id}"
                    )
                finally:
                    # Print a dot to the console to indicate progress
                    print(".", end="", flush=True)

            for alarm_id in alarms_id_list:
                executor.submit(query_single_rules, alarm_id)
                executor.submit(query_single_periods, alarm_id)

            executor.shutdown(wait=True)

        return (
            alarms_dict,
            alarm_rules_dict,
            alarm_periods_dict,
        )

    def get_alarm_rules(self, alarm_id):
        """
        Retrieve alarm rules for a specified organization ID.

        Parameters:
        - alarm_id (str): The ID of the alarm to retrieve data for.


        Returns:
        - dict: Alarm rules.
        """
        url = f"{self.base_url}alarms/{alarm_id}/alarmrules/"
        response = self.get_request_data(url)
        del response["alarmrules"][0]["links"]
        return response["alarmrules"][0]

    def get_alarm_periods(self, alarm_id):
        """
        Retrieve alarm periods for a specified organization ID.

        Parameters:
        - alarm_id (str): The ID of the alarm to retrieve data for.


        Returns:
        - dict: Alarm periods.
        """
        url = f"{self.base_url}alarms/{alarm_id}/alarmperiods/"
        response = self.get_request_data(url)
        del response["alarmperiods"][0]["links"]
        return response["alarmperiods"][0]

    def get_events_list(self, organization_id, date_range=None):
        """
        Retrieve events for a specified organization ID.
        Parameters:
         - organization_id (str): The ID of the organization to retrieve data for.
        Returns:
         - dict: Organization events.
        """
        if not date_range or isinstance(date_range, int):
            url = f"{self.base_url}events/?organization={organization_id}&daterange[]=today&limit=0"
        elif isinstance(date_range, (list, tuple)):
            url = f"{self.base_url}events/?organization={organization_id}&daterange[]={date_range[0]}&daterange[]={date_range[1]}&limit=100"
        elif isinstance(date_range, str):
            url = f"{self.base_url}events/?organization={organization_id}&daterange[]={date_range}&limit=100"

        response = self.get_request_data(url)
        if response["meta"]["pageCount"] == 0 and len(response["events"]) != 0:
            return response["events"]
        elif response["meta"]["pageCount"] != 0 and len(response["events"]) != 0:
            pages = response["meta"]["pageCount"]
            event_pages = response["events"]
            with ThreadPoolExecutor(max_workers=pages) as executor:

                def query_single(page):
                    newurl = f"{url}&page={page}"
                    try:
                        response = self.get_request_data(newurl)
                        event_pages.extend(response["events"])
                    except Exception as e:
                        print(f"\nError while reading event list at page: {page}")
                    finally:
                        # Print a dot to the console to indicate progress
                        print(".", end="", flush=True)

                for page in range(2, pages + 1):
                    executor.submit(query_single, page)

            return event_pages
        else:
            return response
