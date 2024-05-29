import sqlite3
import json
import re
import pandas as pd
from datetime import datetime

'''
1. Get SMS
2. match regex
3. get tag
4. get fields
5. make chnages in database
'''

class operations:

    def __init__(self) -> None:
        
        self.sms=""
        self.sender=""
        self.date=""
        self.fields=["CURRENCY","AMOUNT","ACCOUNT NO","RECIPIENT","DAY","MONTH","YEAR","NOT NEEDED","TRANSACTION NO","BALANCE","HOUR","MIN","AM/PM"]
        self.connection_obj = sqlite3.connect('finanace.db')
        self.cursor_obj = self.connection_obj.cursor()

        self.total_expense=0
        self.total_income=0

    def insert_data(self,table_name:str,*args):
        print(table_name)
        s=""
        l=[]
        command=f"""insert into {table_name} values("""
        for param in args:
            command=command+"?,"
            l.append(param)

        command=command[:-1]
        command=command+""");"""
        print(command,l)
        self.cursor_obj.execute(command,tuple(l))

    def getSMS(self):

        self.sms="Received Rs.600.00 in your a/c 91XX3635 from One97 Communications Limited on 6-5-2022.Ref no: 5C05RE03uMN5. Queries? Call 01204456456 :PPBL"
        self.sender="PAYTMB"
        self.date="26/05/2024"

    def matchRegex(self)->int:
        
        #get all the codes of the organisation to check
        codes=self.cursor_obj.execute(f"""select rowid,* from sender where organisationName in (select organisationName from sender where code="{self.sender}");""")

        for rowid,code,org in codes:

            #get regex under each code
            reg=self.cursor_obj.execute(f"""select * from regex where sender="{code}";""")

            for id,pattern,fieldOrder,debit,sender,tag in reg:
                patt=re.compile(pattern,re.IGNORECASE)
                match=re.match(patt,self.sms)
                if match and match.end()-match.start()==len(self.sms):
                    return id
                
        return -1
    
    def change_accounts_table(self,debit:bool,field_val:list,account_no:int):
        resp=self.cursor_obj.execute(f"""select balance,tag from accounts where accountNo={account_no}""")
        print("CHANGE ACCOUNTS")

        #if account no is present in database
        if resp.fetchone()!=None:
            balance,tag=resp.fetchone()
            if debit:
                balance-=float(field_val[1])
            else:
                balance+=float(field_val[1])

            self.cursor_obj.execute("update accounts set balance= ? where accountNo = ?;",(balance,account_no))

        #PROBLEM: ADD CONDITION for other tags
        else:
            balance=0
            if debit:
                balance-=float(field_val[1])
            else:
                balance+=float(field_val[1])
            command=f"""insert into accounts values(?,?,?,?);"""

            self.cursor_obj.execute(command,(account_no,"Rs",balance,"BANK ACCOUNT"))

    
    def validate_data(self,field_val:list,debit:bool,tag:str)->bool:

        #get last sms row_id
        id=self.cursor_obj.execute("select max(rowid) from not_spam_sms;")

        id=id.fetchone()
        
        for temp in id:
            id=temp
            break
        

        #transactionID
        transactionID=field_val[8]

        if field_val[0]==None or field_val[1]==None:
            return False
        
        amount=field_val[1]
        currency=field_val[0]

        account_no=field_val[2]
        recipient=field_val[3]
        if tag=="BANK TRANSFER":
        
            #account number is not a numeric datatype 
            if field_val[2].isnumeric()==False:
                return False
            

            
            #check account no:
            if field_val[2]!=None:
                check=self.cursor_obj.execute(f"""select * from accounts where accountNo={field_val[2]}""")

                #check if recipient is an account number
                if field_val[3].isnumeric():
                    check2=self.cursor_obj.execute(f"""select * from accounts where accountNo={field_val[3]}""")

                    #if recipient is not an account number then debit or credit
                    if check2.fetchone()==None and debit!=None:
                        if debit:
                            self.total_expense+=int(field_val[1])
                        else:
                            self.total_income+=int(field_val[1])

                    #else do no change expense/income if recipient is your account no... Add/deduct amount from that account
                    #PROBLEM. IF MSG FOR THIS EXPENDITURE ARRIVES AS WELL THERE MIGHT BE DOUBLE TRANSACTION IN DATABASE
                    elif check2.fetchone()!=None and debit!=None:
                        self.change_accounts_table(not debit,field_val,field_val[3])
                
                #add transaction to accounts table
                self.change_accounts_table(debit,field_val,field_val[2])
                    

        #parse date
        date=None
        if field_val[4] and field_val[5] and field_val[6]:
            #PROBLEM: CHECK DATETIME IF YEAR IS YY OR YYYY
            date=datetime.strptime(f"{field_val[4]}/{field_val[5]}/{field_val[6]}","%d/%m/%Y")

        self.insert_data("bank_transactions",id,transactionID,currency,amount,account_no,recipient,date)
        self.connection_obj.commit()

        return True

        
        

                
    def getFields(self,regexID:int):
        #insert the sms in not_spam_sms table
        #PROBLEM: DO NOT REMEMBER THE FUNCTION OF CATEGORY AND TRANSACTIONid 
        self.insert_data("not_spam_sms",self.sms,self.sender,self.date,0,regexID)
        #self.cursor_obj.execute(f"""insert into not_spam_sms values({self.sms},{self.sender},{self.date},0,{regexID},0);""")

        #get data from regex table
        resp=self.cursor_obj.execute(f"""select * from regex where id={regexID};""")
        id,pattern,fieldOrder,debit,sender,tag=resp.fetchone()
        print(pattern)

        #create the pattern object
        patt=re.compile(pattern,re.IGNORECASE)
        match=re.match(patt,self.sms)

        print(match)

        #create an array of field order
        fieldOrder=[int(x) for x in fieldOrder.split(",")]

        #get values of fields. Values not present are None
        field_val=[None for _ in range(len(self.fields))]
        idx=0
        for group in match.groups():
            print(idx)
            field_val[fieldOrder[idx]]=group
            idx+=1

        

        validation_passed=self.validate_data(field_val,debit,tag)

    def main(self):
        self.getSMS()
        id=self.matchRegex()
        print(id)
        if id!=-1:
            self.getFields(id)


obj=operations()
obj.main()
