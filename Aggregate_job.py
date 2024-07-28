
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# khởi tạo spark session
spark = SparkSession.builder.appName("Daily transactions processing") \
            .config("spark.jars", "postgresql-42.6.2.jar") \
            .getOrCreate()

# khai báo thông tin DB
url = 'jdbc:postgresql://localhost:5432/postgres?currentSchema=dwh'
properties = {
    "user": "postgres",
    "password": "login1234",
    "driver": "org.postgresql.Driver"
}

### khai báo tên các bảng dữ liệu
acc_table = 'account'
trans_table = 'transaction'
currency_table = 'currency'
product_table = 'product'

### cách 1: sử dụng query để extract dữ liệu từ nhiều bảng trong cùng 1 câu lệnh query
'''
query = f"""
    (
    SELECT a.account_number , a.customer_id , a.opening_date , a.product_id , p.product_name , a.currency , c.rate , t.trans_amount , t.channel , t.booking_date 
    from {acc_table} a 
    join {trans_table} t  
    on a.account_number = t.account_number
    join {currency_table} c 
    on a.currency = c.currency 
    join {product_table} p 
    on a.product_id = p.product_id 
    where booking_date = '{booking_date}'
) as subquery
"""
df = spark.read.jdbc(url= url, table = query, properties= properties)
df.show()
df.printSchema()
'''

### CÁCH 2: query từng bảng con, sau đó join các DF với nhau
# Bảng account
account = f"""
(SELECT * from {acc_table}) as subquery
"""
acc_df = spark.read.jdbc(url= url, table = account, properties= properties)

# Bảng Currency
currency = f"""
(SELECT * from {currency_table}) as subquery
"""
currency_df = spark.read.jdbc(url= url, table = currency, properties= properties)
currency_df = currency_df.withColumnRenamed('currency', 'currency_code')

# Bảng product
product = f"""
(SELECT * from {product_table}) as subquery
"""
product_df = spark.read.jdbc(url= url, table = product, properties= properties)
product_df = product_df.withColumnRenamed('product_id', 'product_code')

# bảng customer + target
customer_db = 'customer'
target_db = 'target'

customer_query = f"""
    (SELECT c.*, target_name from {customer_db} c left join {target_db} t on c.target_code = t.target_code
    )
"""
customer_df = spark.read.jdbc(url= url, table = customer_query, properties= properties)
customer_df = customer_df.withColumnRenamed('customer_id', 'customer_code')

# chạy dữ liệu từ ngày dến ngày

start_date = datetime.strptime('2024-02-01', '%Y-%m-%d')
end_date = datetime.strptime('2024-06-30', '%Y-%m-%d')

current_date = start_date
while current_date <= end_date: 
    day_value = str(current_date.day).zfill(2)
    month_value = str(current_date.month).zfill(2)
    year_value =  str(current_date.year)
    booking_date = year_value+'-'+month_value+'-'+day_value
    
    # Bảng transaction
    trans_query = f"""
        (SELECT * from {trans_table} where booking_date = '{booking_date}') as subquery
    """
    trans_df = spark.read.jdbc(url= url, table = trans_query, properties= properties)
    trans_df = trans_df.withColumnRenamed('account_number', 'account_id')
    # trans_df.show()

    # join các DF với nhau
    summary_df = acc_df.join(customer_df, acc_df['customer_id'] == customer_df['customer_code'], how='inner')\
                        .join(trans_df, acc_df['account_number'] == trans_df['account_id'], how ='left')\
                        .join(product_df, acc_df['product_id'] == product_df['product_code'], how ='left')\
                        .join(currency_df, acc_df['currency'] == currency_df['currency_code'], how = 'left')
                    # .selectExpr("account_number")#, 'acc_df.customer_id', 'acc_df.opening_date', 'acc_df.product_id', 'acc_df.currency',
                                # 'trans_df.trans_amount', 'trans_df.channel', 'trans_df.booking_date')
    summary_df = summary_df.selectExpr('account_number', 'customer_id', 'full_name', 'target_code', 'target_name', 'gender', 'dob', 'segment',
                                    'opening_date', 'product_id','product_name', 'currency', 'rate',
                                'trans_amount', 'channel', 'booking_date')
    
    # INSERT dữ liệu vào bảng đích
    destination_table = 'dwh.rptb_transaction'

    if summary_df is not None:
        print(f"Processing data on {booking_date}")
        summary_df.write.jdbc(url=url, table=f"{destination_table}", mode='append', properties=properties)
    else:
        print(f"Error: summary_df is None or empty on {booking_date}")

    current_date += timedelta(days=1)

# Dừng tiến trình Spark
spark.stop()


