import pandas as pd
from sqlalchemy import create_engine


def get_creds(filename="database.txt"):
    f = open(filename, "r")
    lines = f.readlines()

    credentials = dict()
    credentials["username"] = lines[0].replace("\n", "")
    credentials["password"] = lines[1].replace("\n", "")
    credentials["host"] = lines[2].replace("\n", "")
    credentials["port"] = lines[3].replace("\n", "")
    credentials["database"] = lines[4].replace("\n", "")
    f.close()

    return credentials


def load_offers_params_table(credentials, date_from="", date_to="", limit=0):
    """
    Connects with PostgreSQL DB and get
    offers params for selected dates
    """

    engine = create_engine(
        f"postgresql://{credentials['username']}:{credentials['password']}"
        f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
    )

    query = "select * from public.otodom_offers_params where 1=1"

    # Dates
    if len(date_from) > 0 and len(date_to) == 0:
        query = f"{query} and date(create_timestamp) = '{date_from}'"
    elif len(date_from) > 0 and len(date_to) > 0:
        query = f"{query} and date(create_timestamp) between '{date_from}' and '{date_to}'"

    # Limit
    if limit > 0:
        query = f"{query} limit {limit}"

    df = pd.read_sql_query(query, engine)

    return df
