#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from psycopg2 import sql
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    try:
        # lower all the alphabets to avoid errors
        SortingColumnName = SortingColumnName.lower()
        InputTable = InputTable.lower()
        OutputTable = OutputTable.lower()
        n_threads = 5
        TEMP_PART_TABLE_PREF = 'sortedpart'

        # get the min and max to decide range for partitioning
        cur = openconnection.cursor()
        cur.execute(sql.SQL("select MIN({col}), MAX({col}) from {tbl}").format(col=sql.Identifier(SortingColumnName),
                                                                               tbl=sql.Identifier(InputTable)))
        mini, maxi = cur.fetchone()

        # get the range by dividing the max rating by num of threads
        rng = (maxi - mini) / n_threads

        threads = []
        for i in range(n_threads):
            threads.append(threading.Thread(target=rangePartAndSort,args=(InputTable, SortingColumnName, TEMP_PART_TABLE_PREF, mini, rng, i, openconnection)))
            threads[i].start()

        # drop output table if already exist and create new one
        cur.execute(sql.SQL("drop table if exists {op}").format(op = sql.Identifier(OutputTable)))
        cur.execute(sql.SQL("create table {op} (LIKE {iptable} including all)").format(op = sql.Identifier(OutputTable),iptable = sql.Identifier(InputTable)))
        for i in range(n_threads):
            threads[i].join()
            insertFromTbl = TEMP_PART_TABLE_PREF+str(i)
            cur.execute(sql.SQL("insert into {} select * from {}").format(sql.Identifier(OutputTable), sql.Identifier(insertFromTbl)))

        openconnection.commit()

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cur:
            cur.close()

######### HELPER FUNCTIONS ##############

def rangePartAndSort(inputtablename, colToPart, tblPrefix, mini, rng, partNum, openconnection):
    try:
        # Load the cursor to the open socket connection
        cur = openconnection.cursor()

        partition_name = tblPrefix + str(partNum)

        cur.execute(sql.SQL("drop table if exists {}").format(sql.Identifier(partition_name)))
        #find out the lower and upper bound of the ratings value for each partition
        frm_rng = mini+rng*(partNum)
        to_rng = frm_rng+rng

        # for i = 0 the range should be inclusive of the lower bound value to handle edge cases and store in temp table in a sorted way
        if partNum==0:
            cur.execute(
                sql.SQL("create temp table {} as "
                        "select * from {} "
                        "where {col}>=%s and {col}<=%s "
                        "order by {col} ASC")
                    .format(sql.Identifier(partition_name),sql.Identifier(inputtablename), col=sql.Identifier(colToPart))
                ,(frm_rng, to_rng))


        # or else just select the rows which satisfy the condition for_range<ratings<=to_range and place them into their corresponding parition and store in temp table in a sorted way
        else:
            cur.execute(
                sql.SQL("create temp table {} as "
                        "select * from {} "
                        "where {col}>%s and {col}<=%s "
                        "order by {col} ASC")
                    .format(sql.Identifier(partition_name), sql.Identifier(inputtablename), col=sql.Identifier(colToPart))
                ,(frm_rng, to_rng))

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cur:
            cur.close()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        InputTable1 = InputTable1.lower()
        InputTable2 = InputTable2.lower()
        Table1JoinColumn = Table1JoinColumn.lower()
        Table2JoinColumn = Table2JoinColumn.lower()
        OutputTable = OutputTable.lower()
        n_threads = 5
        TEMP_PART_TABLE_PREFIX = 'joinpart'
        cur = openconnection.cursor()
        cur.execute(sql.SQL("select MIN({col}), MAX({col}) from {tbl}").format(col=sql.Identifier(Table1JoinColumn),
                                                                               tbl=sql.Identifier(InputTable1)))
        mini1, maxi1 = cur.fetchone()
        cur = openconnection.cursor()
        cur.execute(sql.SQL("select MIN({col}), MAX({col}) from {tbl}").format(col=sql.Identifier(Table2JoinColumn),
                                                                               tbl=sql.Identifier(InputTable2)))
        mini2, maxi2 = cur.fetchone()

        # selecting the max and min from both the tables to avoid discrepancies
        mini, maxi = min(mini1,mini2), max(maxi1,maxi2)

        # get the range by dividing the max rating by num of threads
        rng = (maxi - mini) / n_threads

        threads = []
        for i in range(n_threads):
            threads.append(threading.Thread(target=joinHelper, args=(
            InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, TEMP_PART_TABLE_PREFIX, mini, rng, i, openconnection)))
            threads[i].start()

        # drop output table if already exist and create new one
        cur.execute(sql.SQL("drop table if exists {op}").format(op=sql.Identifier(OutputTable)))

        # wait for the first thread to complete and then create the output table using its schema
        threads[0].join()
        cur.execute(sql.SQL("create table {op} as select * from {joinpart}")
                    .format(op=sql.Identifier(OutputTable),joinpart=sql.Identifier(TEMP_PART_TABLE_PREFIX+str(0))))

        # now append each thread's output to the final table
        for i in range(1,n_threads):
            threads[i].join()
            insertFromTbl = TEMP_PART_TABLE_PREFIX + str(i)
            cur.execute(sql.SQL("insert into {} select * from {}").format(sql.Identifier(OutputTable),
                                                                          sql.Identifier(insertFromTbl)))

        openconnection.commit()

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cur:
            cur.close()

def joinHelper(inputtable1, inputtable2, column1, column2, tblPrefix, mini, rng, partNum, openconnection):
    try:
        # Load the cursor to the open socket connection
        cur = openconnection.cursor()

        partition_name = tblPrefix + str(partNum)

        cur.execute(sql.SQL("drop table if exists {}").format(sql.Identifier(partition_name)))
        #find out the lower and upper bound of the ratings value for each partition
        frm_rng = mini+rng*(partNum)
        to_rng = frm_rng+rng


        if partNum==0:
            cur.execute(
                sql.SQL("create temp table {prt} as "
                        "(select * from "
                        "(select * from {tbl1} where {col1}>=%s and {col1}<=%s) as t1 "
                        "inner join "
                        "(select * from {tbl2} where {col2}>=%s and {col2}<=%s) as t2 "
                        "on t1.{col1} = t2.{col2})")
                    .format(prt = sql.Identifier(partition_name), tbl1 = sql.Identifier(inputtable1), tbl2 = sql.Identifier(inputtable2),
                            col1=sql.Identifier(column1), col2 = sql.Identifier(column2))
                ,(frm_rng, to_rng, frm_rng, to_rng))


        else:
            cur.execute(
                sql.SQL("create temp table {prt} as "
                        "(select * from "
                        "(select * from {tbl1} where {col1}>%s and {col1}<=%s) as t1 "
                        "inner join "
                        "(select * from {tbl2} where {col2}>%s and {col2}<=%s) as t2 "
                        "on t1.{col1} = t2.{col2})")
                    .format(prt=sql.Identifier(partition_name), tbl1=sql.Identifier(inputtable1),
                            tbl2=sql.Identifier(inputtable2),
                            col1=sql.Identifier(column1), col2=sql.Identifier(column2))
                , (frm_rng, to_rng, frm_rng, to_rng))


    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cur:
            cur.close()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


