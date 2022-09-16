from os.path import dirname, abspath
from dotenv import load_dotenv

path = dirname(abspath(__file__)) + "/.env"
load_dotenv(path)
