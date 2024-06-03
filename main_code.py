from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime
from collections import OrderedDict

app = Flask(__name__)

#Dropping the whole table.
@app.route('/identify/<clean>', methods = ['GET','POST'])
def cleaning(clean):
    conn = sqlite3.connect('customers.db')
    conn.execute("drop table customer")
    conn.commit()
    return {"Message":"Table cleared successfully!."}


@app.route('/identify', methods = ['POST'])
def identify():
    conn = sqlite3.connect('customers.db')
    try:
        conn.execute('''
                create table if not exists customer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phoneNumber TEXT,
                    email TEXT,
                    linkedId INTEGER,
                    linkPrecedence TEXT,
                    createdAt DATETIME,
                    updatedAt DATETIME,
                    deletedAt DATETIME
                )
        ''')
        conn.commit()
    except:
        print("table already created")
    conn.close()

    email_id = request.json['email']
    phone_no = request.json['phoneNumber']

    if phone_no == None and email_id == None:
        return {"error": "Both cannot be Null"}
    if type(phone_no) == int:
        return { "phoneNumber should be a String":"Please use double quotation."}

    conn = sqlite3.connect('customers.db')
    #conn.execute("drop table customer")
    cursor = conn.cursor()
    cursor.execute("select * from customer where email = ? OR phoneNumber = ?",(email_id,phone_no))
    records = cursor.fetchall()

    #This is to check if email and phoneNumber exist or not.
    cursor.execute("select * from customer where email = ?",(email_id,))
    try_1 = cursor.fetchall()
    cursor.execute('select * from customer where phoneNumber = ?', (phone_no,))
    try_2 = cursor.fetchall()
    #Checking if email and phoneNumber exist.
    try_bit_1 = bool(try_1)
    try_bit_2 = bool(try_2)

    #If customer does not exist, register the customer.
    if not records:
        cursor.execute("insert into customer(phoneNumber,email,linkedId,linkPrecedence,createdAt,updatedAt,createdAt) values(?,?,?,?,?,?,?)",(phone_no, email_id, None, 'primary', datetime.now(),datetime.now(),None))
        conn.commit()
        #Getting the id of the new customer.
        cursor.execute("SELECT last_insert_rowid()")
        new_id = cursor.fetchone()[0]

        details = {"contact": {
                            "primaryContatctId": new_id,
                            "emails": [email_id],
                            "phoneNumbers": [phone_no],
                            "secondaryContactIds": []
                             }
                    }
        conn.close()
        return details
    elif email_id == None and phone_no != None: #When email is null and phoneNumber is not Null and exist in table.
        id_key = 0
        total_email = []
        total_phone = []
        second_id = []
        for record in records: #This loop is used to find the id of the primary account.
            if record[1] == phone_no and record[4] == "primary":
                id_key = record[0]
                break
            elif record[1] == phone_no and record[4] == "secondary":
                id_key = record[3]
                break

        cursor.execute("select * from customer")
        records_1 = cursor.fetchall()
        for record in records_1: #This loop is to find the email, phoneNumber and secondary_id if any customer has primary id.
            if  record[3] == id_key and record[4] == "secondary":
                total_email.append(record[2])
                total_phone.append(record[1])
                second_id.append(record[0])
            elif record[0] == id_key and record[4] == "primary":
                total_email.append(record[2])
                total_phone.append(record[1])

        # Removing any duplicate entries.
        total_email = list(set(total_email))
        total_phone = list(set(total_phone))
        second_id = list(set(second_id))
        #Removing any null values.
        total_email = list(filter(None, total_email))
        total_phone = list(filter(None, total_phone))
        second_id = list(filter(None, second_id))
        #my_list = list(filter(None, my_list))

        details_1 = {"contact": {
                            "primaryContatctId": id_key,
                            "emails": total_email,
                            "phoneNumbers": total_phone,
                            "secondaryContactIds": second_id
                             }
                    }
        return details_1
    elif email_id != None and phone_no == None: #When email exist and phoneNumber is null.
        id_key = 0
        total_email = []
        total_phone = []
        second_id = []
        for record in records:
            if record[2] == email_id and record[4] == "primary":
                id_key = record[0]
                break
            elif record[2] == email_id and record[4] == "secondary":
                id_key = record[3]
                break

        cursor.execute("select * from customer")
        records_1 = cursor.fetchall()
        for record in records_1:
            if record[3] == id_key and record[4] == "secondary":
                total_email.append(record[2])
                total_phone.append(record[1])
                second_id.append(record[0])
            elif record[0] == id_key and record[4] == "primary":
                total_email.append(record[2])
                total_phone.append(record[1])

        # Removing any duplicate entries.
        total_email = list(set(total_email))
        total_phone = list(set(total_phone))
        second_id = list(set(second_id))
        # Removing any null values.
        total_email = list(filter(None, total_email))
        total_phone = list(filter(None, total_phone))
        second_id = list(filter(None, second_id))

        details_1 = {"contact": {
            "primaryContatctId": id_key,
            "emails": total_email,
            "phoneNumbers": total_phone,
            "secondaryContactIds": second_id
        }
        }
        return details_1
    elif email_id != None and phone_no != None: #When email and phone_no arre not Null.
        #When email is in table and phoneNumber is not in table.
        # #Then we need to insert a new entry with secondary account.
        if try_bit_1 and not try_bit_2:
            key_id = 0
            total_email = []
            total_phone = []
            second_id = []
            for record in records:
                if record[2] == email_id and record[4] == 'primary':
                    key_id = record[0]
                    break
                elif record[2] == email_id and record[4] == 'secondary':
                    key_id = record[3]
                    break

            cursor.execute("insert into customer(phoneNumber,email,linkedId,linkPrecedence,createdAt,updatedAt,createdAt) values(?,?,?,?,?,?,?)",(phone_no, email_id, key_id, 'secondary', datetime.now(), datetime.now(), None))
            conn.commit()
            #Reading all the records of updated customer table.
            cursor.execute("select * from customer")
            records_1 = cursor.fetchall()
            conn.commit()

            for record in records_1:
                if record[3] == key_id and record[4] == 'secondary':
                    total_email.append(record[2])
                    total_phone.append(record[1])
                    second_id.append(record[0])
                elif record[0] == key_id and record[4] == 'primary':
                    total_email.append(record[2])
                    total_phone.append(record[1])

            #Removing any duplicate entries.
            total_email = list(set(total_email))
            total_phone = list(set(total_phone))
            second_id = list(set(second_id))
            # Removing any null values.
            total_email = list(filter(None, total_email))
            total_phone = list(filter(None, total_phone))
            second_id = list(filter(None, second_id))

            details_1 = {"contact": {
                "primaryContatctId": key_id,
                "emails": total_email,
                "phoneNumbers": total_phone,
                "secondaryContactIds": second_id
            }
            }
            return details_1
        elif not try_bit_1 and try_bit_2: #If email is not found and phoneNumber exist, then we need to create a new entry in cusstomer table.
            key_id = 0
            total_email = []
            total_phone = []
            second_id = []
            for record in records:
                if record[1] == phone_no and record[4] == 'primary':
                    key_id = record[0]
                    break
                elif record[1] == phone_no and record[4] == 'secondary':
                    key_id = record[3]
                    break

            cursor.execute("insert into customer(phoneNumber,email,linkedId,linkPrecedence,createdAt,updatedAt,createdAt) values(?,?,?,?,?,?,?)",(phone_no, email_id, key_id, 'secondary', datetime.now(), datetime.now(), None))
            conn.commit()
            # Reading all the records of updated customer table.
            #cursor.execute("select * from customer where email = ? or phoneNumber = ?", (email_id, phone_no))
            cursor.execute("select * from customer")
            records_1 = cursor.fetchall()
            conn.commit()

            for record in records_1:
                if record[3] == key_id and record[4] == 'secondary':
                    total_email.append(record[2])
                    total_phone.append(record[1])
                    second_id.append(record[0])
                elif record[0] == key_id and record[4] == 'primary':
                    total_email.append(record[2])
                    total_phone.append(record[1])

            #Removing any duplicate entries.
            total_email = list(set(total_email))
            total_phone = list(set(total_phone))
            second_id = list(set(second_id))
            # Removing any null values.
            total_email = list(filter(None, total_email))
            total_phone = list(filter(None, total_phone))
            second_id = list(filter(None, second_id))

            details_1 = {"contact": {
                "primaryContatctId": key_id,
                "emails": total_email,
                "phoneNumbers": total_phone,
                "secondaryContactIds": second_id
            }
            }
            return details_1

        elif try_bit_1 and try_bit_2: #When both email and phone exist in table
            #This requires thinking.
            key_id_1 = 0
            key_id_2 = 0
            cursor.execute("select * from customer")
            records_1 = cursor.fetchall()
            for record in records_1: #Finding the id to which email is attached with.
                if record[2] == email_id and record[4] == 'primary':
                    key_id_1 = record[0]
                    break
                elif record[2] == email_id and record[4] == 'secondary':
                    key_id_1 = record[3]
                    break
            for record in records_1:#Finding id to which phoneNumber is attached with.
                if record[1] == phone_no and record[4] == 'primary':
                    key_id_2 = record[0]
                    break
                elif record[1] == phone_no and record[4] == 'secondary':
                    key_id_2 = record[3]
                    break
            print("The primary id to which email is attached is = ",key_id_1)
            print("The primary id to which phoneNumber is attached is  = ",key_id_2)
            if key_id_1 == key_id_2: #If email and phone is attached to same group of id.
                #Simply append all the records related to the id.
                print("----Both email and phone are of same person-----")
                total_email = []
                total_phone = []
                second_id = []
                for record in records_1:
                    if record[3] == key_id_1 and record[4] == 'secondary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])
                    elif record[0] == key_id_1 and record[4] == 'primary':
                        total_email.append(record[2])
                        total_phone.append(record[1])

                # Removing any duplicate entries.
                total_email = list(set(total_email))
                total_phone = list(set(total_phone))
                second_id = list(set(second_id))
                # Removing any null values.
                total_email = list(filter(None, total_email))
                total_phone = list(filter(None, total_phone))
                second_id = list(filter(None, second_id))

                details_1 = {"contact": {
                    "primaryContatctId": key_id_1,
                    "emails": total_email,
                    "phoneNumbers": total_phone,
                    "secondaryContactIds": second_id
                }
                }
                return details_1
            elif key_id_1 < key_id_2: #When email is found first and PhonNumber is found later.
                print("------Email is primary-------- and linking it to rest!")
                total_email = []
                total_phone = []
                second_id = []
                #Adding records related to id when email is found first. Email id is primary.
                for record in records_1:
                    if record[0] == key_id_1 and record[4] == 'primary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                    elif record[3] == key_id_1 and record[4] == 'secondary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                #Adding and updating records where id related to phone is found.
                for record in records_1:
                    if record[0] == key_id_2 and record[4] == 'primary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                        if record[3] != key_id_1:
                            cursor.execute("update customer set linkedId = ?,linkPrecedence = ? where id = ?",(key_id_1,'secondary',record[0]))
                            conn.commit()

                    elif record[3] == key_id_2 and record[4] == 'secondary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                        if record[3] != key_id_1:
                            cursor.execute("update customer set linkedId = ? where id = ?",(key_id_1,record[0]))
                            conn.commit()
                #Removing any duplicate entries.
                total_email = list(set(total_email))
                total_phone = list(set(total_phone))
                second_id = list(set(second_id))
                #Removing any null values.
                total_email = list(filter(None, total_email))
                total_phone = list(filter(None, total_phone))
                second_id = list(filter(None, second_id))

                details_1 = {"contact": {
                    "primaryContatctId": key_id_1,
                    "emails": total_email,
                    "phoneNumbers": total_phone,
                    "secondaryContactIds": second_id
                }
                }
                return details_1
            elif key_id_1 > key_id_2: #When phoneNumber is found first
                print("Phone_number is primarry key----- And linking rest to it.")
                total_email = []
                total_phone = []
                second_id = []
                #Adding records related to id where phone is found first.
                for record in records_1:
                    if record[0] == key_id_2 and record[4] == 'primary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                    elif record[3] == key_id_2 and record[4] == 'secondary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                #Adding and updating records of id related to email in other group.
                for record in records_1:
                    if record[0] == key_id_1 and record[4] == 'primary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                        if record[3] != key_id_2:
                            cursor.execute("update customer set linkedId = ?, linkPrecedence = ? where id = ?",(key_id_2,'secondary',record[0]))
                            conn.commit()
                    elif record[3] == key_id_1 and record[4] == 'secondary':
                        total_email.append(record[2])
                        total_phone.append(record[1])
                        second_id.append(record[0])

                        if record[3] != key_id_2:
                            cursor.execute("update customer set linkedId = ? where id = ?",(key_id_2,record[0]))
                            conn.commit()

                #Removing any duplicate entries.
                total_email = list(set(total_email))
                total_phone = list(set(total_phone))
                second_id = list(set(second_id))
                #Removing any null values.
                total_email = list(filter(None, total_email))
                total_phone = list(filter(None, total_phone))
                second_id = list(filter(None, second_id))

                details_1 = {"contact": {
                    "primaryContatctId": key_id_2,
                    "emails": total_email,
                    "phoneNumbers": total_phone,
                    "secondaryContactIds": second_id
                }
                }
                return details_1

#if __name__ == '__main__':
    #app.run()
