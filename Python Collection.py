# Databricks notebook source
# MAGIC %md 4 Main data structures in python 
# MAGIC
# MAGIC 1. list    - [1,2,3,4]
# MAGIC 2. Tuple   - (1,2,3,4)
# MAGIC 3. Set     -  {1,2,3,4}
# MAGIC 4. Dictionary  - {"brand":"iphone","model":15,"price":6999}
# MAGIC
# MAGIC   - sequence/order - is important for list & tuple& string  as can be handled with index, however, set & dictionary does not follow sequence 
# MAGIC    - Mutable/ immutable - string is immutale 
# MAGIC                           List is  mutable
# MAGIC                           tuple is immutable 
# MAGIC                           set is mutable
# MAGIC                           Dicttionary Mutable
# MAGIC                           
# MAGIC

# COMMAND ----------

 order = [1,"2013-07-25 00:00:00",11599,"CLOSED"]  # Mutable
 print(type(order))

 order = (1,"2013-07-25 00:00:00",11599,"CLOSED")   # Immutable
 print(type(order))

# COMMAND ----------

 order = [1,"2013-07-25 00:00:00",11599,"CLOSED"]  # Mutable
 #print(order[10])
 #list index out of range

 order[3] = "COMPLETE"
 print(order[3])

 order.append(100)
 order.insert(2,100)
 # Modification is not possibule in tuples


 print(len(order))  # len works for tuple as well
 order.pop()      # it remove the last element
 print(order)

 for x in order:
     print(x)



# COMMAND ----------

######################### sum of 1 to 10 numbers ##################
#lst = range(1,11)
#print(sum(lst)  )  
#####################################
# OR
#sum =0 
#for i in lst :
#    sum = sum + i
######################################
range_Start = int(input("Enter starting value : "))
range_End = int( input("Enter Ending value : "))
s =0 
for i in range(range_Start,range_End):
    s= s+i

print(s)



# COMMAND ----------

# sum of only decimals or integests from below list

order_amounts = [100,200,None,"invalid",300,400.5]
s = 0
for x in order_amounts:
    if type(x) == int or type(x) == float :
        s = s + x
    else:
        continue

print(s)

#while Loop
i = 0 
sum =0 
while i < len(order_amounts):
    if type(order_amount[i]) == int or type(order_amounts[i]):
        sum = sum + order_amounts[i]
    else:
        continue
    i=i+1
print(sum)


# COMMAND ----------

# find index value 

order = [1,"2013-07-25 00:00:00",11599,"CLOSED"]

print(order.index(11599))
print(11599 in order)

print(123 in order)

# COMMAND ----------

orders = [50,50,40,50,30,50]
orders.sort()
print(orders)
orders.reverse()
print(orders)
print(orders.count(50))

# COMMAND ----------

orders = [50,50,40,50,30,50]
orders_new = orders
orders_new[1] = 200
print(orders_new)
print(orders)
#[50, 200, 40, 50, 30, 50]
#[50, 200, 40, 50, 30, 50]

##orders & orders_new points to same memory location hence 200 updated in both the lists, in order have seperate copy, we have to use copy method

orders_new = orders.copy()  # it maintain seperate list

# COMMAND ----------

# find list of unique customer from below list

customer_id = [102,105,102,107,110,109,105,102,107]
unique_cid = []
for i in customer_id:
    if i in unique_cid:
        continue
    else:
        unique_cid.append(i)
print(unique_cid)

# alternative solution - one liner - set does not allow duplcates   
print(list(set(customer_id)))

# COMMAND ----------

  ### calculate GST for the list items
  order_amounts = [100,200,300,500,1200,80,10]
  orders_including_gst = []
  for x in order_amounts :
      orders_including_gst.append(x+x * .18)
  print(orders_including_gst)
      

# COMMAND ----------

# MAGIC %md ##list comprehension

# COMMAND ----------

  ### calculate GST for the list items
  order_amounts = [100,200,300,500,1200,80,10]
  orders_including_gst = [ x+x*.18 for x in order_amounts ]
  print(orders_including_gst)
      

# COMMAND ----------

### tuple calculation
## asume (amount,gst)

order_amounts = [(100,5),(200,8),(500,15),(400,20)]
orders_including_gst = []
for x in order_amounts:
    orders_including_gst.append(x[0]+x[0]*x[1]/100)
print(orders_including_gst)

# list comprehension

orders_including_gst = [ i[0]+i[0]*i[1]/100 for i in order_amounts ]
print(orders_including_gst)

# COMMAND ----------

# create nested list  [[1,1,1][2,4,8][3,9,27]]

nested_list = [[i,i**2,i**3] for i in range(1,4)]

print(nested_list)

##flatten the list
flatten_list = []
for sub_list in nested_list:
    for item in sub_list:
        flatten_list.append(item)

print(flatten_list)

flatten_list = [ item for sub_list in nested_list for item in sub_list]

print(flatten_list)


# COMMAND ----------

orders_list = [
    [101,'2023-07-20 00:00:00.0',11590,'CLOSED'],
    [102,'2023-07-20 00:00:00.0',259,'PENDING_PAYMENT'],
    [103,'2023-07-20 00:00:00.0',11220,'COMPLETE'],
]

closed_orders = [ orders for orders in orders_list if orders[3]=='CLOSED' ]
print(closed_orders)

# COMMAND ----------

# MAGIC %md ## unpacking 

# COMMAND ----------

orders = [101,'2023-07-20 00:00:00.0',11590,'CLOSED']
customer_id = orders[0] 
order_date = orders[1]
order_amount = orders[2]
order_status = orders[3]

print(customer_id,order_date,order_amount,order_status)

#### same can be achivabule with below

customer_id,order_date,order_amount,order_status = orders
print(customer_id,order_date,order_amount,order_status)


# COMMAND ----------

# MAGIC %md ## Slicing 

# COMMAND ----------

customer = [1,"Tirupal","XXXXXXXXX","XXXXXXXXXXXXX","6303 Health Plaza","Brownville","TX",78521]
orders = [101,'2023-07-20 00:00:00.0',11590,'CLOSED']
customer[0:2]
customer[0:2]+customer[4:6]
##Append two list
#orders.append(customer)
#print(orders)
#[101, '2023-07-20 00:00:00.0', 11590, 'CLOSED', [1, 'Tirupal', 'XXXXXXXXX', 'XXXXXXXXXXXXX', '6303 Health Plaza', 'Brownville', 'TX', 78521]]

orders.extend(customer)

print(orders)

# COMMAND ----------

# MAGIC %md print each element with index

# COMMAND ----------

for i,e in enumerate(orders):
    #print(i,e)
    print(f'Index: {i}, Element :{e}')

# COMMAND ----------

#Count the number of occurances for each word

input_list = ["tirupal","muchivolu","srikalahshi","TIRUPALU"]
#OR
i= input("Enter some statement : ")

input_list = i.split(" ")

input_list_lower = [ i.lower() for i in input_list]

input_list_lower=list(set(input_list_lower))

input_list_lower = [ (word,input_list_lower.count(word)) for word in input_list_lower]

print(input_list_lower)


# COMMAND ----------

from collections import Counter
l1 = ['CLOSED', 'PENDING_PAYMENT', 'COMPLETE', 'CLOSED', 'COMPLETE', 'COMPLETE', 'COMPLETE', 'PROCESSING', 'PENDING_PAYMENT', 'PENDING_PAYMENT']
counter = Counter(l1)
l2 = [(i, j) for i, j in counter.items()]

print(l2)

# COMMAND ----------

l1 = ['CLOSED', 'PENDING_PAYMENT', 'COMPLETE', 'CLOSED', 'COMPLETE', 'COMPLETE', 'COMPLETE', 'PROCESSING', 'PENDING_PAYMENT', 'PENDING_PAYMENT']

# Create a dictionary to count occurrences
counts = {}
for item in l1:
    if item in counts:
        counts[item] += 1
    else:
        counts[item] = 1

counts

# Convert the dictionary to a list of tuples using list comprehension
#l2 = [(key, counts[key]) for key in counts]

#print(l2)


# COMMAND ----------

ls1 = [
    [101, 'John', 'IT', 60000],
    [102, 'Alice', 'HR', 50000],
    [103, 'Bob', 'Finance', 70000],
    [104, 'Emma', 'IT', 55000],
    [105, 'David', 'Finance', 75000],
    [106, 'Sophia', 'HR', 48000]
]

print(ls1)

# COMMAND ----------


ls1
all_Value = {}

for i in ls1:
    dept = i[2]
    salary = i[3]
    k=[dept,salary]
    all_Value[dept] = i[2]
    all_Value[dept]['Salary'] = i[3]
    print(all_Value)

    
    




# COMMAND ----------

# List of employee records
employees = [
    [101, 'John', 'IT', 60000],
    [102, 'Alice', 'HR', 50000],
    [103, 'Bob', 'Finance', 70000],
    [104, 'Emma', 'IT', 55000],
    [105, 'David', 'Finance', 75000],
    [106, 'Sophia', 'HR', 48000]
]

# Dictionary to store total salaries and counts for each department
department_salaries = {}

# Iterate through the employee records
for emp in employees:
    department = emp[2]
    salary = emp[3]
    
    if department in department_salaries:
        department_salaries[department]['total_salary'] += salary
        department_salaries[department]['count'] += 1
    else:
        department_salaries[department] = {'total_salary': salary, 'count': 1}
print(department_salaries)
# Calculate and print the average salary for each department
#for department, values in department_salaries.items():
#    average_salary = values['total_salary'] / values['count']
#    print(f"{department}: Average Salary - {average_salary:.2f}")


# COMMAND ----------

# MAGIC %md Dictionary

# COMMAND ----------

# MAGIC %md 
# MAGIC Dictionary - Mutable {"one":"1","two":"2"}
# MAGIC
# MAGIC  --> Dictionary is mutable
# MAGIC  --> we can change  the value for particular key
# MAGIC  --> we can add more entries
# MAGIC  --> we can remove some of the entries
# MAGIC
# MAGIC  --> key is immutable

# COMMAND ----------

word_dict = {"intelligent":"the one who is really brialliant"}

word_dict["intelligent"]

# COMMAND ----------



# COMMAND ----------

orders_Data = [(100,1000),(200,2000),(30,1000)]
order_dict = dict(orders_Data)

print(order_dict.keys)

print(order_dict.values)

for order in order_dict:
    print(order)

# COMMAND ----------


