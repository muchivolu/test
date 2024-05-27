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


