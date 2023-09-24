class Atm():

    def __init__(self) -> None:
        self.pin ='' 
        self.balance = 0
        self.menu()
        

    def menu(self):
        input_value=int(input("""
        How can i help you 
        enter 1 for login
        enter 2 for Pay
        enter 3 for Add to Wallet
        enetr 4 for check Balance
        enter 5 for change pin
        enter 6 for Exit\n"""))

        if input_value ==1:
            self.login()
        elif input_value ==2:
            self.pay()
        elif input_value ==3:
            self.addtowallet()
        elif input_value ==4:
            self.checkbalance()
        elif input_value ==5:
            self.changepin()
        elif input_value ==6:
            exit()
            
    def login(self):
        pin=int(input("Enter pin: "))
        self.pin=pin
        print("Login Sucessfull")
        self.menu()

    def changepin(self):
        pin = int(input("Enter the pin: "))
        if self.pin ==pin:
            pin=int(input("Enter the New pin: "))
            self.pin = pin
            self.menu()
        else:
            print("Incorrect Pin!!")
            self.menu()
    
    def addtowallet(self):
        old_pin = int((input("Enter the pin: ")))
        if self.pin == old_pin:
            print("Login Sucessfull")
            amount = int((input("Enter the amount: ")))
            self.balance+=amount
            print("Amount added sucessfully")
            self.menu()
        else:
            print("Incorrect Pin!!")
            self.menu()
    
    def checkbalance(self):
        old_pin = int((input("Enter the pin: ")))
        if self.pin == old_pin:
            print("Wallet Balance is: {}".format(self.balance))
            self.menu()
        else:
            print("Incorrect Pin!!")
            self.menu()

    def pay(self):
        old_pin = int((input("Enter the pin: ")))
        if self.pin == old_pin:
            amount = int((input("Enter the amount: ")))
            if amount > self.balance:
                print("Insufficient Funds")
            else:
                self.balance-=amount
                print("Transaction sucessfull")
            self.menu()
        else:
            print("Incorrect Pin!!")
            self.menu()

        
sbi=Atm()
sbi.checkbalance()
# bob =Atm()