import os


TESTING = True if os.environ.get('TESTING') == "1" else False
