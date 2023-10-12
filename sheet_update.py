# %%
# script which takes summary sheet and upodate feedback loop documents
# version 0.1
from google.oauth2.service_account import Credentials
import gspread
import time, datetime
import pandas as pd
import os, ast, pprint
import random


# authenticate with Google service accout and return client
def gs_authentificate():
    cred_file = "feedbackloop-399807-300aec3efe37.json"
    # Load your credentials from service account file
    creds = Credentials.from_service_account_file(
        cred_file, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    # create a client to interact with the Google Drive API
    return gspread.authorize(creds)


# function which retry on failure
def retry_on_failure(num_attempts=5, timeout=5, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, num_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"{current_time()} Attempt {attempt} failed with error: {e}")
                    if "The service is currently unavailable" in str(e):
                        print(f"{current_time()} Applying exponential backoff.")
                        time.sleep(
                            (2**attempt) + random.random()
                        )  # exponential backoff
                    else:
                        time.sleep(delay)  # regular delay
                    if attempt == num_attempts:
                        print(f"{current_time()} Max attempts reached. Giving up.")
                        raise

        return wrapper

    return decorator


# define function whihc update formate for certain cell range in the worksheet


# define function which open Google Spreadsheet by ID and Worksheet by name and update it wiht data from pandas dataframe
@retry_on_failure(num_attempts=3, timeout=5, delay=1)
def gs_sheet_update(client, spreadsheet_id, worksheet_name, range_name, df):
    # try to open the spreadsheet by SPREADSHEET_ID by client
    try:
        sheet = client.open_by_key(spreadsheet_id)
        print(f"{current_time()}Spreadsheet {sheet.title} opened")
    except:
        print(f"{current_time()}Spreadsheet not found!")

    # try to open a worksheet by WORKSHEET_NAME if not successful create a new worksheet
    try:
        worksheet = sheet.worksheet(worksheet_name)
        print(f"{current_time()}Worksheet {worksheet_name} opened")
    except:
        try:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="10")
            print(f"{current_time()}Worksheet not found. New worksheet created.")
        except Exception as e:
            print(f'{current_time()} {e}')

    if worksheet != None:
        values = [df.columns.values.tolist()] + df.values.tolist()
        # update worksheet with data from dataframe
        try:
            worksheet.update(values=values, range_name=range_name)
        except Exception as e:
            print(f"{current_time()} {e}")
        time.sleep(1)
        # update format for cell range
        cell_format = {"numberFormat": {"type": "TIME", "pattern": "hh:mm"}}
        try:
            worksheet.format(ranges="G:G", format=cell_format)
        except Exception as e:
            print(f'{current_time()} {e}')
            print(f"{current_time()}Error while updating format for cell range!")

        return True


def current_time():
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]: ")


def write_default_config(config_file, default_config):
    with open(config_file, "w") as f:
        f.write(pprint.pformat(default_config))


def read_config(config_file, default_config):
    if not os.path.exists(config_file):
        write_default_config(config_file, default_config)
        org_sheets = default_config
    else:
        with open(config_file, "r") as f:
            content = f.read()
            try:
                org_sheets = ast.literal_eval(content)
                return org_sheets
            except ValueError:
                print(
                    f"{current_time()}Error: Configuration file is not a valid Python dictionary!"
                )
                return None


# %%
# Specify the spreadsheet ID and the range you want to read.
org_sheets = {
    "Burger King Vallecas": "1B5NnGu3MYbNP7-jHAxf2sVg0hhrIU9KhYAJ8ptsa79c",
    "Burger King Benalmádena": "1YtpTU2ZQInhx86FL5Q_X9m58PD0OPLDHjhBwu6UKrOQ",
}

live_org_sheets = {
    "Burger King Vallecas": "1bGMfw-px38zTrlJWEbSEHoDMSLvHi1RdLUAtvBEWyeo",
    "Burger King Benalmádena": "1HZyEPxhQugoRGgvzqKP2ja736bxjhJCF-gJ3gbFH4pg",
}


WORKSHEET_NAME = "HW_MINUTES"
RANGE_NAME = "A1"  # Example range

# temporary gspread warning suppression
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="gspread")


# %%
# Read esxisting hwsummary file and create dataframe

df = pd.read_excel("./reports/hwminutes_summary.xlsx")
df["Alarm"] = df["Alarm"].apply(lambda x: x[: x.index(" (")].capitalize())
df["Active Time, HH:mm"] = (
    pd.to_timedelta(df["Active Time, HH:mm"].apply(lambda x: x + ":00")).astype(int)
    / 1e9
    / (24 * 60 * 60)
)

# try to read config file, if return None, print error message and exit
config_file = "fbl_files.conf"
default_config = org_sheets
live_org_sheets = read_config(config_file, default_config)
if live_org_sheets == None:
    print(f"{current_time()}Error: Configuration file load error!")
    exit()


# %%

try:
    client = gs_authentificate()

    print(f"{current_time()}Client authenticated.")
except:
    print(f"{current_time()}Authentication failed!")

for org in live_org_sheets.keys():
    df_org = df[df["Organization"] == org]
    try:
        gs_sheet_update(
            client, live_org_sheets[org], WORKSHEET_NAME, RANGE_NAME, df_org
        )
        print(f"{current_time()}Update of {org} successful.")
    except:
        print(f"{current_time()}Update of {org} failed!")
    finally:
        time.sleep(1)
