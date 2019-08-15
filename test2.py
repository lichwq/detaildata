 #斐波那契数列
 2 class Fabs():
 3     def __init__(self,max):
 4         self.max=max
 5         self.n,self.a,self.b=0,0,1
 6
 7     def __iter__(self):#定义__iter__方法
 8         return self
 9
10     def __next__(self):#定义__next__方法
11         if self.n<self.max:
12             tmp=self.b
13             self.a,self.b=self.b,self.a+self.b
14             #等价于：
15             #t=(self.a,self.a+self.b)
16             #self.a=t[0]
17             #self.b=t[1]
18             self.n+=1
19             return tmp
20         raise StopIteration
21
22 print(Fabs(5))
23 for item in Fabs(10):
24     print(item,end=' ')