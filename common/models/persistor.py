import logging
import os

class Persistor:
    CHECK_GUARD = "CHECK\n"

    def __init__(self, file_name):
        self.file_name = file_name
        with open(file_name, 'a+') as f: pass # Crea el archivo si no existe
       
    def persist(self, text):
        with open(self.file_name, "a") as f:
            f.write(f"{text}\n")
            f.write(self.CHECK_GUARD)

    def read(self):
        with open(self.file_name, "r") as f:
            persisted_state = f.readlines() 

            while persisted_state and persisted_state[-1] != self.CHECK_GUARD:
                persisted_state.pop() # Me elimino posibles escrituras a medias (si me caigo en medio de una escritura)
            return persisted_state

    def update(self, text):
        with open(self.file_name, "w") as f:
            f.write(f"{text}\n")
            f.write(self.CHECK_GUARD)

    def log(self, text):
        with open(self.file_name, "a") as f:
            f.write(f"{text}\n")

    def wipe(self):
        with open(self.file_name, "w") as _: pass
