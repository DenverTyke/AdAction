#!/usr/bin/python

import sys
import csv
import time
import os
from pydblite.sqlite import Database, Table

start_sec = time.time()
start_str = time.asctime(time.localtime(time.time()) )
#print ("Local start time :",start_str)

file_name = "fizzbuzz.dat";

dir = os.getcwd()
#print ("File Name is ",dir+'/'+file_name);

proc_id = os.getpid();

try: #open file stream
    file = open(file_name, "r")
except IOError:
    #print ("Unable to open file",file_name)
    sys.exit()

db = Database(":memory:")
table1 = Table('tableOne', db)
table1.create(('pid','INTEGER'),('fileName','BLOB'),('validCnt','INTEGER'),\
('invalidCnt','INTEGER'),('rowCnt','INTEGER'),('procTime','BLOB'),\
('startTime','BLOB'),('stopTime','BLOB'))
table1.open()

table2 = Table('tableTwo', db)
table2.create(('pid','INTEGER'),('rowNum','INTEGER'),('errCode','BLOB'))
table2.open()

table3 = Table('tableThree', db)
table3.create(('pid','INTEGER'),('rowNum','INTEGER'),\
('transId','INTEGER') ,('transLine','BLOB'))
table3.open()

table4 = Table('tableFour', db)
table4.create(('pid','INTEGER'),('rowNum','INTEGER'),\
('transId','INTEGER'),('fizzCnt','INTEGER'),('buzzCnt','INTEGER'),\
('fizzbuzz_cnt','INTEGER'),('luckyCnt','INTEGER'),('integerCnt','INTEGER'))
table4.open()

with open(file_name) as csv_file:
    csv_reader = csv.reader(file, delimiter=',')
    line_cnt = 0
    under20_cnt = 0
    over20_cnt = 0
    notint_cnt = 0
    for row in csv_reader:
        row_len = len(row)
        if(row_len < 20): 
            under20_cnt += 1
            table2.insert(proc_id,line_cnt+1,"Under 20")

        elif(row_len > 20):
            over20_cnt += 1
            table2.insert(proc_id,line_cnt+1,"Over 20")

        else:
            for i in range(0,19):
                str = row[i]
                if(str.isdigit() == 0):
                    #print ("Not digit ",str)
                    notint_cnt += 1
                    table2.insert(proc_id,line_cnt+1,"Not Integer")

                    break 
        line_cnt += 1

        #print("line ",line_cnt,": ",row, "of length ",len(row))

#   Transfrom One
        list_one = row
        string = ""
        for i in range(0,len(row) ):
            if(row[i].isdigit() ): 
                numb = int(row[i])
                if(numb % 15 == 0): list_one[i] = "fizzbuzz"
                elif(numb % 3 == 0 ): list_one[i] = "fizz"
                elif(numb % 5 == 0): list_one[i] = "buzz"
            string += list_one[i]
#       print("T1 :",list_one)
        table3.insert(proc_id,line_cnt,1,string)


#   Transform Two
        list_two = row
        string = ""
        for i in range(0,len(row) ):
            if(row[i].isdigit() ): 
                numb = int(row[i])
                if("3" in row[i] ): list_two[i] = "lucky"
                elif(numb % 15 == 0): list_two[i] = "fizzbuzz"
                elif(numb % 3 == 0 ): list_two[i] = "fizz"
                elif(numb % 5 == 0): list_two[i] = "buzz"
            string += list_two[i]
#       print("T2 :",list_two)
        table3.insert(proc_id,line_cnt,2,string)



#   Transform Three
        fizz_cnt = 0
        buzz_cnt = 0
        fizzbuzz_cnt = 0
        lucky_cnt = 0
        integer_cnt = 0
        for i in range(0,len(row) ):
            if(list_two[i] == "fizzbuzz"): fizzbuzz_cnt += 1
            elif(list_two[i] == "fizz"): fizz_cnt += 1
            elif(list_two[i] == "buzz"): buzz_cnt += 1
            elif(list_two[i] == "lucky"): lucky_cnt += 1
            elif(list_two[i].isdigit() ): integer_cnt += 1
        #print("T3 fizz ",fizz_cnt," buzz ",buzz_cnt," fizzbuzz ",fizzbuzz_cnt,\
        #" lucky ",lucky_cnt," integer ",integer_cnt)
        table4.insert(proc_id,line_cnt,fizz_cnt,buzz_cnt,fizzbuzz_cnt\
        ,lucky_cnt,integer_cnt)

    db.commit()
	
    invalid_cnt = under20_cnt + over20_cnt + notint_cnt
    valid_cnt = line_cnt - invalid_cnt

file.close()
#print("Read file")

file_name = dir+'/'+file_name
#print ("File Name is ",file_name);

#print("File line count ",line_cnt) 
#print("valid line count ",valid_cnt) 
#print("invalid line count ",invalid_cnt) 
#print("Over 20 count ",over20_cnt) 
#print("Under 20 count ",under20_cnt) 
#print("Not Integer count ",notint_cnt) 

stop_sec = time.time()
proc_time = stop_sec - start_sec
#print("Process time :",proc_time)

stop_str = time.asctime(time.localtime(time.time()) )
#print ("Local stop time :",stop_str)

table1.insert(proc_id,file_name,valid_cnt,invalid_cnt,line_cnt, \
proc_time,start_str,stop_str)

db.commit()

sys.exit()

