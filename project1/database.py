from urllib.parse import urlparse, uses_netloc
import configparser
import psycopg2

# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your PostgreSQL database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    uses_netloc.append("postgres")
    url = urlparse(connection_string)

    conn = psycopg2.connect(database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port,
        host=url.hostname)

    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS customers( id SERIAL PRIMARY KEY, firstName VARCHAR(50), lastName VARCHAR(50), street VARCHAR(100), city VARCHAR(50), state VARCHAR(20), zip VARCHAR(5))')

    cursor.execute('CREATE TABLE IF NOT EXISTS products( id SERIAL PRIMARY KEY, name VARCHAR(50), price FLOAT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, customerId INT, productId INT, date DATE, FOREIGN KEY (customerId) REFERENCES customers(id) ON DELETE CASCADE ON UPDATE CASCADE, FOREIGN KEY (productId) REFERENCES products(id) ON DELETE CASCADE ON UPDATE CASCADE)')
    conn.commit()

    return conn

def get_customers():
    listToReturn = list()

    cursor = conn.cursor()
    cursor.execute('SELECT id, firstName, lastName, street, city, state, zip FROM customers')
    allCustomers = cursor.fetchall()
    conn.commit()

    for one in allCustomers:
        listToReturn.append({'id': one[0], 'firstName': one[1], 'lastName':one[2], 'street':one[3], 'city':one[4], 'state':one[5], 'zip':one[6]})
    return listToReturn

def get_customer(id):
    cursor = conn.cursor()
    cursor.execute('SELECT id, firstName, lastName, street, city, state, zip FROM customers WHERE id=%s;', (int(id), ))
    customer = cursor.fetchone()
    conn.commit()

    dataToReturn ={'id': customer[0], 'firstName': customer[1], 'lastName':customer[2], 'street':customer[3], 'city':customer[4], 'state':customer[5], 'zip':customer[6]}
    return dataToReturn

def upsert_customer(customer):
    cursor = conn.cursor()

    if 'id' not in customer:
        cursor.execute('INSERT INTO customers(firstName, lastName, street, city, state, zip) VALUES (%s, %s, %s, %s, %s, %s)', (customer['firstName'], customer['lastName'], customer['street'], customer['city'], customer['state'], customer['zip']) )
    else:
        cursor.execute('UPDATE customers SET firstName = %s, lastName = %s, street = %s, city = %s, state = %s, zip = %s WHERE id = %s;', (customer['firstName'],  customer['lastName'], customer['street'], customer['city'], customer['state'], customer['zip'], customer['id']) )

    conn.commit()

def delete_customer(id):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM customers WHERE id =%s;', ( int(id), ))
    conn.commit()

def get_products():
    listToReturn = list()

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM products')
    allProducts = cursor.fetchall()
    conn.commit()

    for one in allProducts:
        listToReturn.append({'id': one[0], 'name': one[1], 'price': one[2]})
    return listToReturn

def get_product(id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM products WHERE id =%s;', (int(id), ))
    prod = cursor.fetchone()
    conn.commit()

    dataToReturn = {'id': prod[0], 'name': prod[1], 'price': prod[2]}
    return dataToReturn

def upsert_product(product):
    cursor = conn.cursor()

    if 'id' not in product:
        cursor.execute('INSERT INTO products(name, price) VALUES(%s, %s)', (product['name'], product['price']))
    else:
        cursor.execute('UPDATE products SET name = %s, price = %s WHERE id = %s;', (product['name'], product['price'], product['id']) )

    conn.commit()

def delete_product(id):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM products WHERE id =%s;', (int(id), ))
    conn.commit()

def get_orders():
    listToReturn = list()

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM orders')
    allOrders = cursor.fetchall()
    conn.commit()

    for one in allOrders:
        tempCustomerId = one[1]
        customerData = get_customer(tempCustomerId)
        tempProductId = one[2]
        productData = get_product(tempProductId)
        listToReturn.append({'id': one[0], 'customerId': one[1], 'productId': one[2], 'date': one[3], 'customer': customerData, 'product': productData })

    return listToReturn

def get_order(id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM orders WHERE id =%s;', (int(id), ))
    theOrder = cursor.fetchone()
    conn.commit()

    tempCustomerId = theOrder[1]
    customerData = get_customer(tempCustomerId)
    tempProductId = theOrder[2]
    productData = get_product(tempProductId)
    dataToReturn ={'id': theOrder[0], 'customerId': theOrder[1], 'productId': theOrder[2], 'date': theOrder[3], 'customer': customerData, 'product': productData }

    return dataToReturn

def upsert_order(order):
    cursor = conn.cursor()
    if 'id' not in order:
        cursor.execute('INSERT INTO orders(customerId, productId, date) VALUES (%s, %s, %s)', (order['customerId'], order['productId'], order['date']) )
    conn.commit() #Can't update order here. To update, update either customer or product

def delete_order(id):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM orders WHERE id =%s;', (int(id), ))
    conn.commit() #Can't update order here. To update, update either customer or product

# Return the customer, with a list of orders.  Each order should have a product
# property as well.
def customer_report(id):
    customer = get_customer(id)
    orders = get_orders()
    customer['orders'] = [o for o in orders if o['customerId'] == id]
    return customer

def purchase_report():
    customers = get_customers()

    for i in customers:
        i['total_money_spent'] = 0

    for customerOne in customers:
        orders = [o for o in get_orders() if o['customerId'] == customerOne['id']]
        orders = sorted(orders, key=lambda k: k['date'])

        for i in orders:
            tempProduct = get_product(i['productId'])
            customerOne['total_money_spent'] += tempProduct['price']

        totalOrders = len(orders)

        if totalOrders > 0:
            customerOne['name'] = customerOne['firstName'] + " " + customerOne['lastName']
            customerOne['last_order_date'] = orders[-1]['date']
            customerOne['total_purchases'] = totalOrders
        else:
            customerOne['name'] = customerOne['firstName'] + " " + customerOne['lastName']
            customerOne['last_order_date'] = ' '
            customerOne['total_purchases'] = 0
            customerOne['total_money_spent'] = 0

    return customers


def sales_report():
    products = get_products()

    for product in products:
        orders = [o for o in get_orders() if o['productId'] == product['id']]
        orders = sorted(orders, key=lambda k: k['date'])

        if len(orders) > 0:
            product['last_order_date'] = orders[-1]['date']
            product['total_sales'] = len(orders)
            product['gross_revenue'] = product['price'] * product['total_sales']
        else:
            product['last_order_date'] = ' '
            product['total_sales'] = 0
            product['gross_revenue'] = 0
    return products

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

conn = initialize();
