import logging
import happybase

class Loader:
    def __init__(self, namenode):
        self.namenode = namenode

    def get_connection(self):
        return happybase.Connection(self.namenode)
