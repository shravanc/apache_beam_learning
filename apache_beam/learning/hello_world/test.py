import re

a = "2f0544e0e64958de8c656135fcdbad1af1b43808c0d0b974fc77f171694c558e swyfthome [02/Oct/2020:10:34:27 +0000] 173.252.87.14 - DD7672BEA6F6C3E1 REST.GET.OBJECT Model01/viewer/3seater/elephant.html 'GET /Model01/viewer/3seater/elephant.html HTTP/1.1' 403 AccessDenied 243 - 11 - '-' 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)' - svJFIFBw+IzQfHQBNcNbBnSgjxAwkjMszsXnMzFV49Ma1O5Xm0zQwEQbAEaJ1wVAxJTRyTWpQU0= - ECDHE-RSA-AES128-GCM-SHA256 - swyfthome.s3-eu-west-1.amazonaws.com TLSv1.2"


output = a.split(' ')
print(output)


import pandas as pd

log = "/tmp/logs/2020-10-02-11-34-19-EA6C5E314B70B157"
df = pd.read_csv(log, delimiter=' ')
print(df.head())
