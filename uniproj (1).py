import mysql.connector
from mysql.connector import Error

def create_connection(database):
    try:
        connection = mysql.connector.connect(
            host = 'uni-project.cp2ldanyjuww.us-east-2.rds.amazonaws.com',
            database = database,
            user = 'admin',
            password = 'qwer1234!'
        )
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

    except mysql.connector.Error as error:
        print("Failed to connect in MySQL: {}".format(error))
    return connection

 

def create_tabledb(c):
    try:
        sql_create_Stathmos = """CREATE TABLE Stathmos_paragwghs (
                                Id_stathmou INT NOT NULL,
                                Topothesia VARCHAR(250) NOT NULL,
                                PRIMARY KEY (Id_stathmou))"""

        sql_create_Komvos = """CREATE TABLE Komvos (
                                Id_komvou INT NOT NULL AUTO_INCREMENT,
                                Topothesia VARCHAR(255) NOT NULL,
                                Katastash_leit INT NOT NULL,
                                Enarks_leit DATETIME,
                                Lhksh_leit DATETIME,
                                PRIMARY KEY(Id_komvou))"""

        sql_create_Roloi = """CREATE TABLE Roloi (
                                Id_rologiou INT NOT NULL,
                                Endeiksh_katanalwshs INT NOT NULL,
                                Katastash_leit INT NOT NULL,
                                Id_komvou INT,
                                PRIMARY KEY(Id_rologiou),
                                CONSTRAINT Roloi_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou))""" #Id komvou FK sto komvo

        sql_create_Aisththras = """CREATE TABLE Aisththras (
                                Id_aisththra VARCHAR(255) NOT NULL,
                                Suxnothta_metrhshs INT NOT NULL,
                                Id_komvou INT NOT NULL,
                                Id_sundeshs INT NOT NULL,
                                PRIMARY KEY(Id_aisththra),
                                CONSTRAINT Aisththras_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou) ON DELETE CASCADE,
                                CONSTRAINT Aisththras_fk_Id_sundeshs FOREIGN KEY (Id_sundeshs)
                                    REFERENCES Sundesh (Id_sundeshs) ON DELETE CASCADE)""" #Id komvou FK sto komvo
                                                                         #Suxnothta_metrhshs einai se second

        sql_create_Sundesh = """CREATE TABLE Sundesh (
                                Id_sundeshs INT NOT NULL AUTO_INCREMENT,
                                Id_komvou1 INT NOT NULL,
                                Id_komvou2 INT NOT NULL,
                                Mhkos INT NOT NULL,
                                Tupos VARCHAR(255) NOT NULL,
                                Eidos_tashs VARCHAR(255) NOT NULL,
                                Katastash_leit INT NOT NULL,
                                PRIMARY KEY(Id_sundeshs),
                                CONSTRAINT Sundesh_fk_Id_komvou1 FOREIGN KEY (Id_komvou1)
                                    REFERENCES Komvos (Id_komvou) ON DELETE CASCADE,
                                CONSTRAINT Sundesh_fk_Id_komvou2 FOREIGN KEY (Id_komvou2)
                                    REFERENCES Komvos (Id_komvou) ON DELETE CASCADE)"""
        
        sql_create_Metrhsh = """CREATE TABLE Metrhsh (
                                Id_metrhshs INT NOT NULL AUTO_INCREMENT,
                                Id_aisththra VARCHAR(255) NOT NULL,
                                Xronos_metrhshs TIMESTAMP NOT NULL,
                                Posothta_volt DECIMAL NOT NULL,
                                Posothta_watt DECIMAL NOT NULL,
                                PRIMARY KEY(Id_metrhshs),
                                CONSTRAINT Metrhsh_fk_Id_aisththra FOREIGN KEY (Id_aisththra)
                                    REFERENCES Aisththras (Id_aisththra) ON DELETE CASCADE)"""

        sql_create_Paragwgh = """CREATE TABLE Paragwgh (
                                Id_stathmou INT NOT NULL,
                                Posothta DECIMAL NOT NULL,
                                PRIMARY KEY(Id_stathmou),
                                CONSTRAINT Paragwgh_fk_Id_stathmou FOREIGN KEY (Id_stathmou)
                                    REFERENCES Stathmos_paragwghs (Id_stathmou))"""

        sql_create_Trofodosia = """CREATE TABLE Trofodosia (
                                Id_stathmou INT NOT NULL,
                                Id_komvou INT NOT NULL,
                                PRIMARY KEY(Id_stathmou, Id_komvou),
                                CONSTRAINT Trofodosia_fk_Id_stathmou FOREIGN KEY (Id_stathmou)
                                    REFERENCES Stathmos_paragwghs (Id_stathmou),
                                CONSTRAINT Trofodosia_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou))""" 
        
        sql_create_Katevasths = """CREATE TABLE Katevasths (
                                Id_komvou INT NOT NULL,
                                PRIMARY KEY(Id_komvou),
                                CONSTRAINT Katevasths_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou))"""
        
        sql_create_Anupswths = """CREATE TABLE Anupswths (
                                Id_komvou INT NOT NULL,
                                PRIMARY KEY(Id_komvou),
                                CONSTRAINT Anupswths_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou))"""

        sql_create_Metaforas = """CREATE TABLE Metaforas (
                                Id_komvou INT NOT NULL,
                                PRIMARY KEY(Id_komvou),
                                CONSTRAINT Metaforas_fk_Id_komvou FOREIGN KEY (Id_komvou)
                                    REFERENCES Komvos (Id_komvou))"""

        cursor = c.cursor()
        table_stathmos = cursor.execute(sql_create_Stathmos)
        table_komvos = cursor.execute(sql_create_Komvos)
        table_roloi = cursor.execute(sql_create_Roloi)
        table_sundesh = cursor.execute(sql_create_Sundesh)
        table_aisththras = cursor.execute(sql_create_Aisththras)
        table_metrhsh = cursor.execute(sql_create_Metrhsh)
        table_Paragwgh = cursor.execute(sql_create_Paragwgh)
        table_Trofodosia = cursor.execute(sql_create_Trofodosia)
        table_Katevasths = cursor.execute(sql_create_Katevasths)
        table_Anupswths = cursor.execute(sql_create_Anupswths)
        table_Metaforas = cursor.execute(sql_create_Metaforas)

        print("Tables created successfully")
    
    except Error as e:
        print("Failed to create tables in MySQL: {}".format(e))
    finally:
        if (c.is_connected()):
            cursor.close()

def drop_tables(c):
    try:
        sql_drop_stathmos = "DROP TABLE IF EXISTS Stathmos_paragwghs"
        sql_drop_komvos = "DROP TABLE IF EXISTS Komvos"
        sql_drop_roloi = "DROP TABLE IF EXISTS Roloi"  
        sql_drop_aisththras = "DROP TABLE IF EXISTS Aisththras"
        sql_drop_sundesh = "DROP TABLE IF EXISTS Sundesh"
        sql_drop_metrhsh = "DROP TABLE IF EXISTS Metrhsh"
        sql_drop_paragwgh = "DROP TABLE IF EXISTS Paragwgh"
        sql_drop_trofodosia = "DROP TABLE IF EXISTS Trofodosia"
        sql_drop_katevasths = "DROP TABLE IF EXISTS Katevasths"
        sql_drop_anupswths = "DROP TABLE IF EXISTS Anupswths"
        sql_drop_metaforas = "DROP TABLE IF EXISTS Metaforas"
        cursor = c.cursor()
        cursor.execute(sql_drop_metrhsh)
        cursor.execute(sql_drop_aisththras)
        cursor.execute(sql_drop_sundesh)
        cursor.execute(sql_drop_paragwgh)
        cursor.execute(sql_drop_trofodosia)
        cursor.execute(sql_drop_katevasths)
        cursor.execute(sql_drop_anupswths)
        cursor.execute(sql_drop_metaforas)
        cursor.execute(sql_drop_stathmos)
        cursor.execute(sql_drop_roloi)
        cursor.execute(sql_drop_komvos)
        print("Tables dropped")
    
    except Error as e:
        print("Failed to drop tables", e)
    finally:
        if (c.is_connected):
            cursor.close()

def select_statements(c):
    loop = True
    cursor = c.cursor()
    while loop:
        opt = input("\n(1)Ολες οι πληροφοριες ενος κομβου,\n(2)Ποιοι κομβοι δε λειτουργουν,\n(3)Avg_τιμες Αισθητηρα,\n(4)Αμεσες συνδεσεις ενος κομβου,\n(5)Ολα τα στοιχεια ενος αισθητηρα,\n(6)Γραμμες με ασυνηθιστες τιμες,\n(7)Κομβοι μεταφορας που υπολειτουργουν,\n(end)ends selects...")
        if not opt: break
        if opt == '1':
            user_input = input("insert id_komvou : ")
            select_allkomvos(c, user_input)           
        if opt == '2':
            select_komvosnotworking(c)
        if opt == '3':
            user_input = input("insert id_komvou : ")
            if len(user_input) == 1:
                select_averages_of_aisththres(c, user_input)
        if opt == '4':
            user_input = input("insert id_komvou : ")
            select_ameses_sundeseis(c, user_input)
        if opt == '5':
            user_input = input("insert id_aisththra : ")
            allresults_aisththra(c, user_input)
        if opt == '6':
            sundeseis_asunhthistes(c)
        if opt == '7':
            komvoi_upoleit(c)
        if opt == 'end':
            loop = False

#Τυπώνει        
def select_allkomvos(c, id):
    if id.isdigit(): id = int(id)
    try:
        cursor = c.cursor()
        sql_allforkomvos = """Select * from Komvos WHERE Id_komvou = %s"""
        cursor.execute(sql_allforkomvos, (id,))
        result = cursor.fetchall()
        for x in result:
            print(x)
        cursor.close()
    except Error as e:
        print("Failed to select from Komvo", e)           

def select_komvosnotworking(c):
    try:
        cursor = c.cursor()
        sql_notkomvos = """SELECT Id_komvou,Topothesia,Enarks_leit,Lhksh_leit
                           FROM Komvos
                           WHERE Katastash_leit = 0 """  
        cursor.execute(sql_notkomvos)
        result = cursor.fetchall()
        for x in result:
            print(x)
        cursor.close()       
    except Error as e:
        print("Failed to retrieve data", e)

def select_averages_of_aisththres(c, id):
    if id.isdigit(): id = int(id)
    try:
        cursor = c.cursor()
        sql_average = """SELECT Aisththras.Id_komvou,Aisththras.Id_aisththra,CAST(Metrhsh.Xronos_metrhshs AS date) AS Hmeromhnia,AVG(Metrhsh.Posothta_volt) AS Mesh_timh_volt,AVG(Metrhsh.Posothta_watt) AS Mesh_timh_watt
                         FROM  Aisththras JOIN Metrhsh 
                             ON Aisththras.Id_aisththra=Metrhsh.Id_aisththra
                         JOIN Sundesh
                             ON Sundesh.Id_sundeshs=Aisththras.Id_sundeshs
                         WHERE Aisththras.Id_komvou=%s 
                         GROUP BY CAST(Metrhsh.Xronos_metrhshs AS date),Aisththras.Id_aisththra """
        cursor.execute(sql_average, (id,))
        result = cursor.fetchall()
        print("\nPrinting averages of aisththra")
        for row in result:
            print("Id_komvou = ", row[0])
            print("Id_aisththra = ", row[1])
            print("Date = ", row[2])
            print("Avg_volt = ", row[3])
            print("Avg_watt = ", row[4], "\n")
        cursor.close()
    except Error as e:
        print("Didnt find averages", e)

def select_ameses_sundeseis(c, id):
    if id.isdigit(): id = int(id)
    try:
        cursor = c.cursor()
        sql_ameshsund = """ SELECT IF(Id_komvou1=%s,Id_komvou2,Id_komvou1) AS Komvos
                            FROM Sundesh
                            WHERE ((Id_komvou1 =%s) OR (Id_komvou2 =%s))"""
        cursor.execute(sql_ameshsund, (id,id,id,))
        result = cursor.fetchall()
        for x in result:
            print(x)
        cursor.close()
    except Error as e:
        print("Couldnt find ameses sundeseis...", e)

def allresults_aisththra(c, id):
    try:
        cursor = c.cursor()
        sql_allres = """SELECT Aisththras.Id_aisththra, Metrhsh.Xronos_metrhshs , Sundesh.Id_sundeshs, Metrhsh.Posothta_volt, Metrhsh.Posothta_watt
                        FROM  Aisththras 
                        JOIN Metrhsh 
                            ON Aisththras.Id_aisththra=Metrhsh.Id_aisththra
                        JOIN Sundesh
                            ON Sundesh.Id_sundeshs=Aisththras.Id_sundeshs
                        WHERE  Aisththras.Id_aisththra = %s
                        ORDER BY Sundesh.Id_sundeshs """
        cursor.execute(sql_allres, (id,))
        result = cursor.fetchall()
        print("\nPrinting all results of aisththra")
        for row in result:
            print("Id_aisththra = ", row[0])
            print("Xronos_metrhshs = ", row[1])
            print("Id_sundeshs = ", row[2])
            print("Posothta_volt = ", row[3])
            print("Posothta_watt = ", row[4], "\n")
            cursor.close()
    except Error as e:
        print("Didnt find results", e)
        
def insert_komvos(c, topothesia):
    sql_ins_komv = """INSERT INTO Komvos (Topothesia, Katastash_leit, Enarks_leit, Lhksh_leit)
                 VALUES
                 (%s, 1, NOW(), NULL)"""
    try:
        cursor = c.cursor()
        cursor.execute(sql_ins_komv, (topothesia,))
        c.commit()
        last_id = cursor.lastrowid
        print("Komvos inserted successfully with id : ", last_id)
        cursor.close()

        print("\nConnect Komvos to another Komvo")
        arithmos_sundesewn = input("poses sundeseis thelete : ")
        for i in range(int(arithmos_sundesewn)):
            sundesh_komvou = input("\nKomvos to connect to : ")
            id_sundeshs = insert_sundesh(c, last_id, sundesh_komvou)
            insert_aisththra(c, last_id, id_sundeshs)
            

    except Error as e:
        print("not ok", e)
          


def insert_sundesh(c, inserted_komvos, komvos_sundeshs):
    sql_ins_sund = """INSERT INTO Sundesh (Id_komvou1, Id_komvou2, Mhkos, Tupos, Eidos_tashs, Katastash_leit)
                 VALUES 
                 (%s, %s, 70, 'Ypogeia', 'Ypsilh', 1)"""
    try:
        cursor = c.cursor()
        cursor.execute(sql_ins_sund, (inserted_komvos,komvos_sundeshs,))
        c.commit()
        last_id_sundeshs = cursor.lastrowid
        cursor.close()

    except Error as e:
        print("Couldnt put sundesh", e)
    return last_id_sundeshs

def insert_aisththra(c, id_komvou, id_sundeshs):
    sql_ins_aisththra = """INSERT INTO Aisththras (Id_aisththra, Suxnothta_metrhshs, Id_komvou, Id_sundeshs)
                        VALUES
                        ('%sA%s', 30, %s, %s)"""
    try:
        cursor = c.cursor()
        cursor.execute(sql_ins_aisththra, (id_komvou, id_sundeshs, id_komvou, id_sundeshs,))
        c.commit()
        cursor.close()

    except Error as e:
        print("Couldnt put aisththres", e)


def delete_komvos(c, id):
    if id.isdigit(): id = int(id)
    try:
        sql_dkomvos = """DELETE FROM Komvos WHERE Id_komvou = %s"""

        sql_select_query = """SELECT * FROM Komvos WHERE Id_komvou = %s"""

        cursor = c.cursor()
        cursor.execute(sql_dkomvos, (id,))
        c.commit()

        cursor.execute(sql_select_query, (id,))
        records = cursor.fetchall()
        if len(records) == 0:
            print("\nKomvos Deleted successfully")

        cursor.close()
    except Error as e:
        print("Cant delete komvos : ", e)

def sundeseis_asunhthistes(c):
    sql_state = """SELECT C.Sundesh,C.Id_KomvA,C.Id_KomvB,C.PosoA,C.PosoB,CAST(C.Xronos_Metrhshs as date)
                    FROM(
                    SELECT A.Id_SundA AS Sundesh,A.Id_KomvA,B.Id_KomvB,A.PosoA,B.PosoB,A.Xronos_MetA AS Xronos_Metrhshs
                    FROM (SELECT Aisththras.Id_aisththra AS Id_AisA,Metrhsh.Xronos_metrhshs AS Xronos_MetA ,Sundesh.Id_sundeshs AS Id_SundA,Metrhsh.Posothta_volt AS PosoA ,Aisththras.Id_komvou AS Id_KomvA
                    FROM  Aisththras JOIN Metrhsh 
                        ON Aisththras.Id_aisththra=Metrhsh.Id_aisththra
                    JOIN Sundesh
                        ON Sundesh.Id_sundeshs=Aisththras.Id_sundeshs
                    ) AS A
                    JOIN (SELECT Aisththras.Id_aisththra AS Id_AisB ,Metrhsh.Xronos_metrhshs AS Xronos_MetB,Sundesh.Id_sundeshs AS Id_SundB,Metrhsh.Posothta_volt AS PosoB ,Aisththras.Id_komvou AS Id_KomvB
                    FROM  Aisththras JOIN Metrhsh 
                        ON Aisththras.Id_aisththra=Metrhsh.Id_aisththra
                    JOIN Sundesh
                        ON Sundesh.Id_sundeshs=Aisththras.Id_sundeshs
                    ) AS B
                    ON A.Id_SundA=B.Id_SundB AND A.Id_AisA!=B.Id_AisB
                    WHERE ABS(PosoA/PosoB - PosoB/PosoA)> 0.15 
                    GROUP BY B.Id_SundB,A.Xronos_MetA
                    ) AS C
                    GROUP BY C.Sundesh , CAST(C.Xronos_Metrhshs as date)
                    HAVING COUNT(C.Sundesh)>0"""
    try:
        cursor = c.cursor()
        cursor.execute(sql_state)
        myselect = cursor.fetchall()

        for x in myselect:
            print (x)
        
        cursor.close()

    except Error as e:
        print("Couldnt compute ",e)

def komvoi_upoleit(c):
    sql_komvupol = """SELECT Aisththras.Id_komvou,CAST(Metrhsh.Xronos_metrhshs as DATE),AVG(Posothta_watt)
                        FROM Aisththras JOIN Metrhsh ON Aisththras.Id_aisththra=Metrhsh.Id_aisththra
                        WHERE Aisththras.Id_komvou IN (SELECT Id_komvou FROM Komvos WHERE Katastash_leit = 1) AND Aisththras.Id_komvou IN (SELECT Id_komvou FROM Metaforas) 
                        GROUP BY Aisththras.Id_komvou,CAST(Metrhsh.Xronos_metrhshs as DATE)
                        HAVING AVG(Posothta_watt)<115 OR ( AVG(Posothta_watt) BETWEEN 250 AND 1250)
                        """
    try:
        cursor = c.cursor()
        cursor.execute(sql_komvupol)
        myselect = cursor.fetchall()

        for x in myselect:
            print (x)
        
        cursor.close()

    except Error as e:
        print("Couldnt compute ",e)



def main():
    database = 'Test_Proj'
    conn = create_connection(database)

    loop = True

    while loop:
        sel = input("\n(1)Εισαγωγη κομβου,\n(2)Διαγραφη κομβου,\n(3)Select statements,\n(4)Δημιουργια βασης,\n(5)Διαγραφη βασης,\n(end)ends program...")
        if not sel: break
        if sel == '1':
            user_input = input("insert topothesia : ")
            insert_komvos(conn, user_input)
        elif sel == '2':
            user_in = input("\npress yes if you are sure:  ")
            if user_in == 'yes':
                user_in_2 = input("\nid to delete: ")
                delete_komvos(conn, user_in_2)
        elif sel == '3':
            select_statements(conn)
        elif sel == '4':
            create_tabledb(conn)
        elif sel == '5':
            user_in = input("\npress yes if you are sure:  ")
            if user_in == 'yes':
                drop_tables(conn)
        elif sel == 'end':
            loop = False
        

if __name__ == '__main__':
    main()
