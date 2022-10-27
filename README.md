import csv
import pandas as pd
import datetime as dt
from flask import Flask, request

pd.set_option('display.max_columns', None)
dataset_borrower = pd.read_csv('C:/Users/annak/Desktop/tabelas/borrower.csv', low_memory=False)
# print (dataset_borrower)

dataset_credit = pd.read_csv('C:/Users/annak/Desktop/tabelas/credit.csv', low_memory=False)
# print (dataset_credit)
#BORROWER
borrower = dataset_borrower[["id", "has_own_house", "has_vehicle"]]
borrower = borrower.dropna(axis=1)
borrower = borrower.rename(columns={"id": "borrower_id"})
#print(borrower)
#CREDIT
credit = dataset_credit[["borrower_id","business_profession", "business_income", "bank", "zip_code", "state", "birthday", "gender", "relationship_status"]]
credit = credit.drop_duplicates(subset = 'borrower_id')
#print(credit)
final_dataset = pd.merge(borrower, credit, on="borrower_id")
final_dataset['birthday'] = (dt.datetime.today()\
                             -pd.to_datetime(final_dataset['birthday'])).astype('timedelta64[Y]')
final_dataset = pd.merge(borrower, credit, on="borrower_id")
final_dataset = final_dataset.rename(columns={"birthday": "age"})

================================================================================================================================================================

app = Flask(__name__)

tabela = pd.read_csv('borrower.csv')
dataset = tabela['borrower_id'].sum()


@app.route("/search", methods=['GET'])
def search():
    return "<p>Tabela!</p>"

@app.route("/create",methods=['POST'])
def create():
    return "<p>Postou a pizza!</p>"

@app.route("/update",methods=['PUT'])
def update():
    return "<p>Updatou a pizza!</p>"

@app.route("/delete",methods=['DELETE'])
def delete():
    return "<p>Deletou a pizza!</p>"


if __name__ == "__main__":
    app.run()
    
=========================================================================================================================================
    
 import psycopg2

#Update connection string information

#configurações
host = "localhost"
dbname = "dataset"
user = "postgres"
password = "Eg290619"
sslmode = " require"

#string de conexão
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

conn = psycopg2.connect(conn_string)

conn = psycopg2.connect(conn_string)
print("Connection established")

cursor = conn.cursor()


def create_user(borrower_id, business_profession, business_income, bank, zip_code, state, birthday, gender, relationship_status):
    cursor.execute("INSERT INTO id (borrower_id, business_profession, business_income, bank, zip_code, state, birthday, gender, relationship_status)"
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (borrower_id, business_profession, business_income, bank, zip_code, state, birthday, gender, relationship_status))

# borrower_id","business_profession", "business_income", "bank",
# "zip_code", "state", "birthday", "gender", "relationship_status

def find_all():
    cursor.execute("SELECT*FROM usuario;")
    return cursor.fetchall()

def find_one(id):
    cursor.execute("SELECT * FROM id WHERE id=%s ;",(id, ))
    return cursor.fetchall()

def update_user(borrower_id, business_profession, business_income, bank, zip_code, state, birthday, gender, relationship_status):
    cursor.execute("UPDATE id SET borrower_id=%s, business_profession=%s, business_income=%s, bank=%s, zip_code=%s, state=%s, birthday=%s, gender=%s, relationship_status=%s WHERE id=%s;",
                   (borrower_id, business_profession, business_income, bank, zip_code, state, birthday, gender, relationship_status))

# (borrower_id=%S, business_profession=%S, business_income=%S, bank=%S, zip_code=%S, state=%S, birthday=%S, gender=%S, relationship_status=%S)

def delete_user(id):
    cursor.execute("DELETE FROM id WHERE id=%s ;",(id,))


delete_user(8)


conn.commit()

# user = find_one(8)
# print(user)

cursor.close()
conn.close()
