import threading
def addA(a):
    a+=1
    return

def printtime():
    print('1s')
    return


def this_func():
    e.wait(1)
    e.set()
    while True:
        print(e.isSet())
        pass
    print('1s')
e = threading.Event()
this_func()

