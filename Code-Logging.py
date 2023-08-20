import pysnooper

@pysnooper.snoop()
def sum(x,y):
    print(x+y)

sum(1,2)