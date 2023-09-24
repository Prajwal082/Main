class Triangle:
    
	def __init__(self,b,h):
		self.base = b
		self.height = h

	# def __str__(self) -> str:
	# 	return '{},{}'.format(self.base,self.height)
	
	def trianglearea(self):
		return self.base * self.height *0.5
	

	def addsum(self):
		return  self.trianglearea(Triangle(6))

ob = Triangle(2,2)
# print(ob)
# print(ob.trianglearea())
# print(ob.addsum())
ob.trianglearea()

