# UtmProxy
Proxy server for universal transport module from Federal Service for Alcohol Market Regulation


UtmProxy downloads all incoming documents (/opt/out) to local DB and save them permanently. Outcoming documents will be saved to DB and redirected to real UTM. All other requests will be transparently redirected to real UTM.


How to use:


1) Edit utmproxy.py and change UTM address, ports and sqlalchemy connection string.

2) Install service by executing "python utmservice.py install"