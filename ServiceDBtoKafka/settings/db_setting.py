from dotenv import load_dotenv
import os

load_dotenv()

def get_secrets_from_file(file_path, secret_from_sysenv):
    try:
        #if file_path added for  case when file_path will be none mean there is no set variable for file and will return none
        if file_path and os.path.exists(file_path):
            with open(f"{file_path}",'r') as file:
                return file.read().strip()
        else:
            return os.getenv(secret_from_sysenv)
    except IOError:
        return os.getenv(secret_from_sysenv)

#it load data for connection string from files, files are in secrets
#paths to this files to secret locaiton are in system_env
#there are also second option, docker-compose contain other set of these variables 
#these 2nd set will be used if file/secret will be not found
DATABASE_URL="postgresql://{user}:{pwd}@{db_host}:5432/{db_name}".format(user=get_secrets_from_file(os.getenv("POSTGRES_USER_FILE"), "POSTGRES_USER"),
                                                                          pwd=get_secrets_from_file(os.getenv("POSTGRES_PASSWORD_FILE"), "POSTGRES_PASSWORD"),
                                                                          db_host=get_secrets_from_file(os.getenv("POSTGRES_DB_LOCALHOST_FILE"), "POSTGRES_DB_LOCALHOST"),
                                                                          db_name=get_secrets_from_file(os.getenv("POSTGRES_DB_FILE"), "POSTGRES_DB"))