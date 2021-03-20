import datetime
import random
import os
import sys
import time
import create_table
import table
import socket
import threading

#qeury type list
#db.select(varname1, varname2 from table);
#db.update(table, varname=x, varname=y);
#db.user.update->username,var1;password,var2->username,varx,password,varxy
#db.delete(table, varname=x);
#db.create(table, properties);

tables = {}
tokens = ['www.idonotcare.com']

class TCP_socket:
    
    def __init__(self,soc):
        self.soc = soc
    

    def new_conn(self):
        command_pls_cred = self.soc.recv(4096).decode().split('::')
        #print(command_pls_cred)
        if len(command_pls_cred) == 3:
            tokens.append('token-9098922028')
            self.soc.send('token-9098922028'.encode('utf-8'))
            return
        
        commad = command_pls_cred[1]
        cred_token =  command_pls_cred[0]
        #print(cred_token)
        if cred_token in tokens:
            cmdp = _command(commad)
            cmdr = cmdp._exec()
            #print(cmdr)
            self.soc.send(str(cmdr).encode('utf-8'))
        else:
            self.soc.send('token error'.encode('utf-8'))
                          
        self.soc.close()
    
                          
class _command:
    
    def __init__(self,cmd):
        self.cmd = cmd
        
    def _exec(self):
        #print('command is :',self.cmd)
        
        if self.cmd == 'q':
            print('system exiting now')
            sys.exit()
        if self.cmd == 'commit':
            return table.commit(tables)
        
        else:
            
            cmdlist = self.cmd.split('.')
            #print(cmdlist)
            if cmdlist[0] == 'db':
                if 'table' == cmdlist[1]:
                    if 'create' == cmdlist[2].split('->')[0]:
                        #print('tbale creating')
                        table.create(tables,self.cmd.split('->')[1],self.cmd.split('->')[2])
                        reload_tables()
                    elif 'drop' == cmdlist[2]:
                        return table.drop(tables,self.cmd.split('->')[1])
                elif tables[cmdlist[1]] != None:
                    if cmdlist[2].split('->')[0] == 'add':
                        return table.insert(tables,cmdlist[1],self.cmd.split('->')[1])
                    elif cmdlist[2].split('->')[0] == 'select':
                        return table.select(tables,cmdlist[1],self.cmd.split('->')[1])
                    elif cmdlist[2].split('->')[0] == 'update':
                        return table.update(tables,cmdlist[1],self.cmd.split('->')[1],self.cmd.split('->')[2])
                    elif cmdlist[2].split('->')[0] == 'delete':
                        return table.remove(tables,cmdlist[1],self.cmd.split('->')[1])
                    else:
                        return 'Command not found [100]'
                else:
                    return 'command not found [101]'

def load_tables_data():
    for tblname, complx in tables.items():
        records = open(complx[0])
        for rcd in records.readlines():
            if len(rcd)<2:
                continue
            else:
                complx[2].append(eval(rcd[:-1]))
        #print(complx)
        records.close()
        
def check_files():
    print('File check started for 1 database ')
    frms = open('data//frm//frm.list')
    for frmline in frms.readlines():
        frmloc = 'data//data//' + frmline[:-1] + '.bin'
        if os.path.exists(frmloc):
            tables[frmline[:-1]] = [str(frmloc),[],[]]
            rfile = 'data//data//' + frmline[:-1] + '.frm.r'
            rf = open(rfile)
            for rz in rf.readlines():
                tables[frmline[:-1]][1].append(rz[:-1])
            #print('form check done for ', frmline[:-1])
            rf.close()
        else:
            print('Error processing : ',frmline[:-1])
            sys.exit()
        frms.close()
    return 'Form check finished 100%'
    
def reload_tables():
    check_files()
    load_tables_data()

def main():
    print('Database started at : ', time.ctime())
    check_files()
    load_tables_data()
    #print(tables)
    sock = socket.socket()
    ip = '127.0.0.1'
    port = 4407
    adr = (ip,port)
    sock.bind(adr)
    sock.listen()
    print('Database sytem ready :::: ver 1.0.0.0')
    while 1:
        cli, addr = sock.accept()
        #print(cli)
        cl1 = TCP_socket(cli)
        th1 = threading.Thread(cl1.new_conn())
        th1.start()
        
 
main()
