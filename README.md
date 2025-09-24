# Window-Creating-withColumn
Its an Interview question
# Question  
Create a new datafrane df1 with the given values  
Count null entries in a datafarme  
Remove null entries and the store the null entries in a new datafarme df2  
Create a new dataframe df3 with the given values and join the two dataframes df1 & df2  
Fill the null values with the mean age all of students  
Filter the students who are 18 years above and older  
<b> </b> 
<b> </b> 
# Solution 
data1 = [(1,'Jhon',17),(2,'Maria',20),(3,'Raj',None),(4,'Rachel',18)]  
columns = ["id", "name", "age"]  

df1 = spark.createDataFrame(data1, schema = columns)  
df1.show()  

nulldf = df1.select([sum(col(column).isNull().cast("int")).alias(column) for column in df1.columns])  
nulldf.show()  

df2 = df1.filter(col("age").isNull())  
df2.show()  

data2 = [(1,'seatle',82),(2,'london',75),(3,'banglore',60),(4,'boston',90)]  
columns2 = ["id", "city", "code"]  

df3 = spark.createDataFrame(data2, schema = columns2)  
df3.show()  

mergedf= df1.join(df3,["id"],"full")  
mergedf.show()  

#fill the null value with the mean age of students  
#calculate the mean age  

from pyspark.sql.window import Window  
from pyspark.sql.functions import *  

createwindow = Window.partitionBy(lit(1))  

meanage = mergedf.withColumn(  
    "age",  
    when (  
        col("age").isNull(),  
        round(avg("age").over(createwindow)).cast("int")  
    ).otherwise(col("age").cast("int"))  
)  

meanage.show()  

#Get the students who are 18 years or older  

fildf = meanage.filter(col("age")>=18)  
fildf.show()  
# Output Data

![Input data](https://github.com/user-attachments/assets/6b6516db-bfcb-47c6-9d0f-ad6ae42eb074)
