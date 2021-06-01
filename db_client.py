import socket
import threading
import socket
import sys


class db:

    def __init__(self,sock,ip,port,datalist):
        self.ip = ip
        self.port = port
        self.sock = sock
        self.sock = socket.socket()


    def close(self):
        try:
            self.sock.close()
        except Exception as ex:
            return ex

    def fetchall(self):
        tempd = self.datalist
        self.datalist = []
        return tempd

    def fetchone(self):
        tempd = self.datalist[0]
        self.datalist = []
        return tempd

    def add(self,table,details):
        #db.user.add->details
        try:
            tr = 'db..{}..add->{}'.format(table,str(details))
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
        except Exception as ex:
            return ex

        self.sock.close()
        
    def delete(self,table,details):
        try:
            tr = 'db..{}..delete->{}'.format(table,str(details))
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
            self.datalist = [None, self.sock.recv(90000).decode()]
        except Exception as ex:
            return ex
            
    def select(self,table,details):

        try:
            #self.sock.close()
            tr = 'db..{}..select->{}'.format(table,str(details))
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
            self.datalist = eval(self.sock.recv(90000).decode())
        
        except Exception as ex:
            return ex

        self.sock.close()

    def update(self,table,details1,details2):
        try: 
            tr = 'db..{}..update->{}->{}'.format(table,str(details1),str(details2))
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
        except Exception as ex:
            return ex
        self.sock.close()

    def create(self,table,rows):

        try:
            tr = 'db..table..create->{}->{}'.format(table,str(rows))
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
        except Exception as ex:
            return ex
        self.sock.close()

    def show(self):
        try:
            tr = 'db..table..show->{}->{}'
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
            self.datalist = eval(self.sock.recv(4096))
            return self.fetchall()
        except Exception as ex:
            return ex
        self.sock.close()

    def describe(self,table):
        try:
            tr = 'db..table..describe->{}'.format(table)
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send(tr.encode('ascii'))
            self.datalist = eval(self.sock.recv(4096))
            return self.fetchall()
        except Exception as ex:
            return ex
        self.sock.close()

    def commit(self):
        try:
            self.sock = socket.socket()
            self.sock.connect((self.ip,self.port))
            self.sock.send('commit'.encode('ascii'))
        except Exception as ex:
            return ex
        self.sock.close()

