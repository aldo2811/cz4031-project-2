# Query Explainer

## Prerequisite:
- Python (3.8+)
- Postgres loaded with database (e.g. TPC-H)

## Preparation
### Setup `.env` file
1. Rename `.env.example` to `.env`
2. Fill `.env` with your credentials

    | Name     | Description                                     |
    |----------|-------------------------------------------------|
    | DB_UNAME | username of user in DB                          |
    | DB_PASS  | password of that user                           |
    | DB_HOST  | address of the DB (e.g. localhost, 192.168.1.5) |
    | DB_PORT  | port number of the DB                           |

##  How to run
1. Setup `.env` file as above
2. Open command prompt, go to project directory
3. Enter the following commands
   1. `pip3 install virtualenv`
   2. `py -3 -m venv venv`
   3. `.\venv\Scripts\activate`
   4. `pip install -r .\requirements.txt`
   5. `python .\project.py`


## Troubleshoot
Q: I don't have `py` installed. 

A: In step 2, use `<path_to_python.exe> -m venv venv` instead.
    Alternatively, look at [this StackOverflow](https://stackoverflow.com/a/5088548).
