import csv
import psycopg2
tr=1
if tr:
    connection = psycopg2.connect(user = "admin",
                                  password = "password",
                                  host = "server url",
                                  port = "1234",
                                  database = "db")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    #print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
                #print("PostgreSQL connection is closed")
    #with open('dump.csv') as csvfile:
    with open('dump.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            x = row[3]
            id = row[0]
            full_parent="%s" %(id)
            #if "/" in x:
            #    y = x.rsplit("/", 1)
            #    full_parent="/%s" %(id)
            # else:
            #    full_parent="%s" %(id)
            #print(full_parent)
            #-----------------------------------------------------------------------
            y=x.rsplit("/", 1)
            if y[0].strip()!="All":
                parent_id1=""
                sql = "SELECT id FROM product_category where complete_name='%s'" % (y[0])
                cursor.execute(sql)
                #print (sql)
                records = cursor.fetchall() 
                for row in records:
                   parent_id1= row[0]
                   #print (parent_id1)
                if parent_id1!="":  
                    full_parent = "%s/%s" % (parent_id1, full_parent)
                    sql = "Update product_category set parent_id = '%s' where id='%s'" % (parent_id1, id)
                    #print("[%s] [%s] [%s] [%s] " % (x, full_parent, full_parent2, id))
                    print("[%s] [%s]"  % (x, sql))
                    #exit("exit")
                    cursor.execute(sql)
                    connection.commit()
                #print(full_parent)
            #-----------------------------------------------------------------------
            y=x.rsplit("/", 2)
            if y[0].strip()!="All":
                parent_id2=""
                sql = "SELECT id FROM product_category where complete_name='%s'" % (y[0])
                cursor.execute(sql)
                #print (sql)
                records = cursor.fetchall() 
                for row in records:
                   parent_id2= row[0]
                   #print (parent_id2)
                if parent_id2!="":  
                    full_parent = "%s/%s" % (parent_id2, full_parent)
            y=x.rsplit("/", 3)
            if y[0].strip()!="All":
               
                parent_id3=""
                sql = "SELECT id FROM product_category where complete_name='%s'" % (y[0])
                cursor.execute(sql)
                #print (sql)
                records = cursor.fetchall() 
                for row in records:
                   parent_id3= row[0]
                   #print (parent_id3)
                if parent_id3!="":   
                   full_parent = "%s/%s" % (parent_id3, full_parent)
                   
            y=x.rsplit("/", 4)       
            if y[0].strip()!="All":
                
                parent_id4=""
                sql = "SELECT id FROM product_category where complete_name='%s' " % (y[0])
                cursor.execute(sql)
                #print (sql)
                records = cursor.fetchall() 
                for row in records:
                   parent_id4= row[0]
                   #print (parent_id4)
                if parent_id4!="":   
                   full_parent = "%s/%s" % (parent_id4, full_parent)
                   
            #if parent_id4!="":
            #    full_parent = "%s" (parent_id4)
            #    + "/" + "parent_id3" + "/" + "parent_id2" + "/" "parent_id1"
            full_parent2 = "1/%s" % (full_parent)
            '''sql = "Update product_category set parent_path = '%s' where id='%s'" % (full_parent2, id)
            #print("[%s] [%s] [%s] [%s] " % (x, full_parent, full_parent2, id))
            print("[%s] [%s]"  % (x, sql))
            #exit("exit")
            cursor.execute(sql)
            connection.commit()'''
#except (Exception, psycopg2.Error) as error :
#    print ("Error while connecting to PostgreSQL", error)
#finally:
#    #closing database connection.
if(connection):
    cursor.close()
    connection.close()
