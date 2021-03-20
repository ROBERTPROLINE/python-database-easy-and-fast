import sys
import os
import threading


def create(tables,mytable,details):
    try:
        #db.table.create->transactions->;userid,text;trid,text;tramt,double;trdetails,text
        #print('Table creating started')

        path = 'data//data//{}.bin'.format(mytable)
        if os.path.exists(path):
            print('table file exists')
        else:
            file = open(path,'w')
            file.close()
            #print('data file created')
            frmdetails = 'data//data//{}.frm.r'.format(mytable) 
            file = open(frmdetails,'w')
            for crz in details.split(';'):
                print(crz)
                crzcode = '{};{}\n'.format(crz.split(',')[0],crz.split(',')[1])
                file.write(crzcode)
            file.close()
            #print('CRZ file created')
            frmlst = 'data//frm//frm.list'
            file = open(frmlst,'a')
            file.write('{}\n'.format(mytable))
            #print('form added to form list')
        #database.reload_tables()
    except Exception as ex:
        print(ex)



def alter(table,alterations):
	pass

def drop(table):
	
    pass

def select(tables,mytable,data):
    if data == 'all':
        return tables[mytable][2]
    else:
        datalist = []
        if ';' in data:
            cnz = data.split(';')
            if len(cnz) == 2:
                for dctz in tables[mytable][2]:
                    if((dctz[cnz[0].split(',')[0]] == cnz[0].split(',')[1]) and (dctz[cnz[1].split(',')[0]] == cnz[1].split(',')[1])):
                        datalist.append(dctz)
                    else:
                        continue
        if '/' in data:
            cnz = data.split('/')
            if len(cnz) == 2:
                for dctz in tables[mytable][2]:
                    if((dctz[cnz[0].split(',')[0]] == cnz[0].split(',')[1]) or (dctz[cnz[1].split(',')[0]] == cnz[1].split(',')[1])):
                        datalist.append(dctz)
                    else:
                        continue
        else:
            for dctz in tables[mytable][2]:
                if(dctz[data.split(',')[0]] == data.split(',')[1]):
                    datalist.append(dctz)
            
            
        return datalist
    #commit(tables)
                    
            
def update(tables,mytable,rdata,updates):
    #db.user.update->#rdata#username,var1;password,var2->#updates#username,varx;password,varxy
    #data####username,robert;password,admin->currentamt,60

    
    try:
        datalist = select(tables,mytable,rdata)
        print(datalist)
        if len(datalist) >= 1:
            if ';' in updates:
                updlist = updates.split(';')
                for rcrdz in datalist:
                    for condition in updlist:
                        var = condition.split(',')[0]
                        vrx = condition.split(',')[1]
                        rcrdz[var] = vrx
                print(datalist)
                for i in tables[mytable][2]:
                    i.update(datalist)
            else:
                for rcrdz in datalist:
                    var = updates.split(',')[0]
                    vrx = updates.split(',')[1]
                    rcrdz[var] = vrx
                print(datalist)
                for i in tables[mytable][2]:
                    i.update(datalist)
        else:
            print('No rows changes')
    except Exception as ex:
        print(ex)
        return None
    commit(tables)
        
        
def insert(tables,mytable,data):
    try:
        #print('inserting mydata : ',data)
        datasize = data.split(',')
        if len(datasize) != len(tables[mytable][1]):
            return 'Row value(s) do not match row count'
        else:
            newrecord = {}
            rowlen = len(tables[mytable][1])
            for ln in range(0,rowlen):
                rowcount = 26
                try:
                    rowcount = int(tables[mytable][1][ln].split(';')[2])
                except Exception as ex:
                    if 'ndexError' in str(ex):
                        rowcount = 1000
                #print('Row counts is ',rowcount)
                if len(data.split(',')) > rowcount:
                    return 'Count Error'
                
                #print(data.split(',')[ln])
                nvalue = ''
                datatype = tables[mytable][1][ln].split(';')[1]
                #print(datatype)
                if datatype == 'double':
                    try:
                        nvalue = float(data.split(',')[ln])
                    except Exception as ex:
                        return 'Value error'
                else:
                    nvalue = str(data.split(',')[ln])
                newrecord[tables[mytable][1][ln].split(';')[0]] = nvalue
                #print('new record : ',newrecord)
            
            tables[mytable][2].append(newrecord)
            return 'Ok [1 row added]'
                        
                #else:
                    #if 'unique' in tables[mytable][1][ln]:
                    #    if tables[mytable][2][data.split(',')[ln]] != None:
                    #        return 'Constraint Error [Unique]'
        commit(tables)
    except Exception as ex:
        print(ex)

def commit(tables):
    
    try:
        for tbl,complx in tables.items():
            tbl_loc = complx[0]
            tbl_dta = complx[2]

            rfile = open(tbl_loc,'w')
            for flz in tbl_dta:
                #print(flz)
                rfile.write(str(flz))
                rfile.write('\n')
            rfile.close()
        return 'Ok Commited'
    except Exception as ex:
        #print(ex)
        return 'Commit Error [100]'
        

def remove():
    pass



##################################

def _select_update():
    pass


def _insert_irr():
    pass

    
