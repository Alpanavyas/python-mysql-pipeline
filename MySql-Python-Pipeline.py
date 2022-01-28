import  logging 
import time

logging.basicConfig(filename ="/home/saif/LFS/logs/exceptions.log")
logger = logging.getLogger()

#setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

import mysql.connector as mysql 
db = mysql.connect(host = "localhost", user = "root", passwd = "Welcome@123", database = "movies")
cursor = db.cursor() 
cursor.execute("CREATE TABLE movie2(movieId INT NOT NULL,Title varchar(200),year varchar(5),genres varchar(500))")

def read_file(path):
    """Return the contents of file at path and time taken"""
    start_time = time.time()
    try:
        file = open("/home/saif/LFS/datasets/movies_1.csv")

        csv_data = csv.reader(file)
        skipHeader = True
        st = []
        for row in csv_data:
            if skipHeader:
                skipHeader = False
                continue
            t = 0
            for j in row:
                if t == 1:
                    string = str(j)
                    for k in range(len(string)):
                        if string[k] == "(":
                            movi = string[0:k]
                            num = string[k+1:k+5]
                            st.append(movi)
                            st.append(num)

                else:
                    st.append(j)  
                t += 1
            cursor.execute("Insert into movie2(movieId,Title,year,genres) values(%s,%s,%s,%s)",st)
            st = []
        db.commit()
        cursor.close()
        db.close()
    except FileNotFoundError as err:
        logger.error(err)
    else:
        file.close()
    finally:
        stop_time = time.time()
        dt = stop_time - start_time
        print('Time required for {file} = {time}'.format(file = path, time = dt))
        print('Process completed')

read_file("/home/saif/LFS/datasets/movies_1.csv")
