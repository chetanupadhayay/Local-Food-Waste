import pymysql


def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="121221",
        database="food_waste"
    )
