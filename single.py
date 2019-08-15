# coding=utf-8
#测试utf-8编码
from time import sleep, time
import sys, threading

reload(sys)
sys.setdefaultencoding('utf-8')


def getNums(N):
    return xrange(N)


def processNum(num):
    num_add = num + 1
    sleep(1)
    print str(threading.current_thread()) + ": " + str(num) + " → " + str(num_add)


if __name__ == "__main__":
    t1 = time()
    for i in getNums(3):
        processNum(i)

    print "cost time is: {:.2f}s".format(time() - t1)