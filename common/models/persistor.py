import logging

class Persistor:
    def __init__(self, file_name):
        self.file_name = file_name
        
    def persist(self, text):
        with open(self.file_name, "a") as f:
            f.write(f"{text}\n")
            f.write("CHECK\n")

    def read(self):
        with open(self.file_name, "r") as f:
            persisted_state = f.readlines()    

            while persisted_state and persisted_state[-1] != "CHECK":
                persisted_state.pop() # Me elimino posibles escrituras a medias (si me caigo en medio de una escritura)

            return persisted_state
