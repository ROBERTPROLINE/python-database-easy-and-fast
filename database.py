import datetime
import os
import sys
import time
import table
import socket
import threading
from sys import exit

#tables = {}
#tokens_url = ['www.idonotcare.com']

class TCP_socket(threading.Thread):
    
    def __init__(self,soc):
        threading.Thread.__init__(self)
        self.soc = soc
    

    def run(self):
        command = self.soc.recv(4096).decode()
        mdp = _command(command)
        cmdr = mdp._exec()
        self.soc.send(str(cmdr).encode('ascii'))
        self.soc.close()
           
                          
class _command:
    
    def __init__(self,cmd):
        self.cmd = cmd
        
    def _exec(self):
        #print('command is :',self.cmd)
        
        if self.cmd == 'q':
            table.commit(tables)
            print('system exiting now')
            sys.exit()
        if self.cmd == 'commit':
            return table.commit(tables)
        
        else:
            
            cmdlist = self.cmd.split('..')
            #print('cmdlist : ',cmdlist)
            if cmdlist[0] == 'db':
                if 'table' == cmdlist[1]:
                    if 'create' == cmdlist[2].split('->')[0]:
                        #print('tbale creating')
                        table.create(tables,self.cmd.split('->')[1],self.cmd.split('->')[2])
                        reload_tables()
                    elif cmdlist[2].split('->')[0] == 'show':
                        return table.show()
                    elif cmdlist[2].split('->')[0] == 'describe':
                        return table.describe(tables,self.cmd.split('->')[1])

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
    try:
        print('Loading data to system')
        for tblname, complx in tables.items():
            records = open(complx[0])
            for rcd in records.readlines():
                if len(rcd)<2:
                    continue
                else:
                    complx[2].append(eval(rcd[:-1]))
            #print(complx)
            records.close()
    except Exception ex:
        print('Error :')
        print('\n')
        print(ex)
        print('\n\nOccured while loading data to system')
        exit()
    print('data loaded 100% ... No Errors')
    
        
def check_files():
    try:
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
        print('Form check finished 100% ... No Errors')
    except Exception ex:
        print('Error :')
        print('\n')
        print(ex)
        print('\n\nOccured while checking data files for system')
        exit()
    
def reload_tables():
    print('#################this.is.a.reload#################')
    check_files()
    load_tables_data()
    print('#################reload.is.done##################')


def main():

    print('Database started at : ', time.ctime())
    check_files()
    load_tables_data()
    #print(tables)

    sock = socket.socket()
    ip = '127.0.0.1'
    port = 9090
    addr = (ip,port)
    sock.bind(addr)
    sock.listen()

    while True:
        clie, adr = sock.accept()
        cl1 = TCP_socket(clie)
        cl1.start()

main()
