class Triangle:  
  def __init__(self,b,h):
    self.base = b
    self.height = h

  def __str__(self) -> str:
    return '{},{}'.format(self.base,self.height)

  def trianglearea(self):
    return self.base * self.height *0.5

  def addnum(self):
    pass


ob = Triangle(2,2)
# print(ob)
ob.trianglearea()
# print(ob.addsum())