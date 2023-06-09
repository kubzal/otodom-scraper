def get_creds():
    f = open("database.txt", "r")
    lines = f.readlines()
    
    credentials = dict()
    credentials["username"] = lines[0].replace("\n", "")
    credentials["password"] = lines[1].replace("\n", "")
    credentials["host"] = lines[2].replace("\n", "")
    credentials["port"] = lines[3].replace("\n", "")
    credentials["database"] = lines[4].replace("\n", "")
    f.close()
    
    return credentials