

class Test():
    def __init__(self):
        print ("costruttore")
    def __del__(self):
        print ("distruttore")

    def __enter__(self):
        print("CM enter")

    def __exit__(self, exception_type, exception_value, traceback):
        print("CM exit")
    
    def __str__(self):
        return "sono str"

with Test() as b:
    print("with a")
    print(b)