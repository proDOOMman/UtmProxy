#!/usr/bin/python

import io
import re
import sched
import sys
import threading
import time
import os
from datetime import datetime, timedelta
from urllib.request import urlopen, HTTPHandler, build_opener, Request
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
from wsgiref.handlers import format_date_time
from time import mktime

import win32serviceutil
import win32service
import win32event
import servicemanager

import multipart as mp
from twisted.internet import reactor, protocol
from twisted.python import log
from twisted.python.logfile import DailyLogFile
from twisted.web import http

from sqlalchemy import Column, DateTime, String, Integer, Boolean, Binary, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

utmHost = u"127.0.0.1"
utmPort = 8080
proxyPort = 8081
deleteDocuments = True

dir_path = os.path.dirname(os.path.realpath(__file__))
log.startLogging(DailyLogFile.fromFullPath(dir_path + "/utmproxy.log"))

Base = declarative_base()


class UtmDocument(Base):
    __tablename__ = 'documents'
    id = Column(Integer, primary_key=True)
    utmPath = Column(String)
    documentType = Column(String)
    replyId = Column(String)
    document = Column(Binary)
    archived = Column(Boolean, default=False)
    ts = Column(DateTime, default=func.now())
    originalId = Column(Integer)


engine = create_engine('sqlite:///' + dir_path + '\\utmproxy.db')
# MSSQL:
# engine = create_engine('mssql+pymssql://user:password@server/database?charset=utf8')
session_factory = sessionmaker(bind=engine)
session = scoped_session(session_factory)
Base.metadata.create_all(engine)


class ProxyClient(http.HTTPClient):

    def __init__(self, method, uri, postData, headers, originalRequest):
        self.method = method
        self.uri = uri
        self.postData = postData
        self.headers = headers
        self.originalRequest = originalRequest
        self.contentLength = None

    def sendRequest(self):
        self.sendCommand(self.method, self.uri)

    def sendHeaders(self):
        for key, values in self.headers:
            if key.lower() == 'connection':
                values = ['close']
            elif key.lower() == 'keep-alive':
                next

            for value in values:
                self.sendHeader(key, value)
        self.endHeaders()

    def sendPostData(self):
        self.transport.write(self.postData)

    def connectionMade(self):
        self.sendRequest()
        self.sendHeaders()
        if self.method == b'POST':
            self.sendPostData()

    def handleStatus(self, version, code, message):
        self.originalRequest.setResponseCode(int(code), message)

    def handleHeader(self, key, value):
        if key.lower() == 'content-length':
            self.contentLength = value
        else:
            self.originalRequest.responseHeaders.addRawHeader(key, value)

    def handleResponse(self, data):
        data = self.originalRequest.processResponse(data)

        if self.contentLength is not None:
            self.originalRequest.setHeader('Content-Length', len(data))

        self.originalRequest.write(data)

        self.originalRequest.finish()
        self.transport.loseConnection()


class ProxyClientFactory(protocol.ClientFactory):
    def __init__(self, method, uri, postData, headers, originalRequest):
        self.protocol = ProxyClient
        self.method = method
        self.uri = uri
        self.postData = postData
        self.headers = headers
        self.originalRequest = originalRequest

    def buildProtocol(self, addr):
        return self.protocol(self.method, self.uri, self.postData,
                             self.headers, self.originalRequest)

    def clientConnectionFailed(self, connector, reason):
        log.err("Server connection failed: %s" % reason)
        self.originalRequest.setResponseCode(504)
        self.originalRequest.finish()


class ProxyRequest(http.Request):
    def __init__(self, channel, queued, reactor=reactor):
        http.Request.__init__(self, channel, queued)
        self.reactor = reactor
        self.document = None

    def process(self):
        self_host = self.host.host
        self_port = self.host.port

        if self.path.startswith(b"/opt/out"):
            if self.path == b"/opt/out" or self.path == b"/opt/out/":
                s = session()
                self.setResponseCode(http.OK)
                self.setHeader("Content-Type", "text/xml;charset=utf-8")
                now = datetime.now()
                stamp = mktime(now.timetuple())
                self.setHeader("Date", format_date_time(stamp))
                self.setHeader("Server", "UTM cached proxy")

                limit = int(self.args.get(b'limit', [0])[0])
                offset = int(self.args.get(b'offset', [0])[0])
                archived = self.args.get(b'archived', [b'0'])[0] == b'1'
                reply_id = self.args.get(b'replyId', [None])[0]
                if archived is True and limit == 0:
                    limit = 500
                top = Element('A')
                query = s.query(UtmDocument)
                if reply_id is not None:
                    query = query.filter(UtmDocument.replyId == reply_id)
                else:
                    query = query.filter(UtmDocument.archived == archived,
                                         UtmDocument.documentType != "request")
                if archived:
                    query = query.order_by(desc(UtmDocument.id))
                else:
                    query = query.order_by(UtmDocument.id)
                if limit > 0:
                    query = query.limit(limit)
                if offset > 0:
                    query = query.offset(offset)
                for doc in query.all():
                    child = SubElement(top, 'url')
                    if doc.documentType == 'request':
                        child.text = "http://%s:%s%s/%s" % (self_host, self_port, doc.utmPath, doc.id)
                    else:
                        child.text = "http://%s:%s/opt/out/%s/%s" % (self_host, self_port, doc.documentType, doc.id)
                    if len(doc.replyId) > 0:
                        child.set("replyId", doc.replyId)
                ver = SubElement(top, 'ver')
                ver.text = '1'
                self.write(tostring(top))

                self.finish()
                s.close()
                return
            else:
                pattern = re.compile(r"/opt/out/(.*)/(\d+)")
                path_array = pattern.findall(str(self.path))

                if len (path_array) == 1 and len(path_array[0]) == 2:
                    s = session()
                    self.setResponseCode(http.OK)
                    now = datetime.now()
                    stamp = mktime(now.timetuple())
                    self.setHeader("Date", format_date_time(stamp))
                    self.setHeader("Server", "UTM cached proxy")

                    query = s.query(UtmDocument).filter(UtmDocument.id == path_array[0][1],
                                                        UtmDocument.documentType == path_array[0][0])
                    if query.count() == 0:
                        self.setResponseCode(404)
                        self.finish()
                        s.close()
                        return
                    document = query.one()
                    if self.method == b'DELETE':
                        document.archived = True
                        s.add(document)
                        s.commit()
                    else:
                        self.setHeader("Content-Type", "text/xml;charset=utf-8")
                        self.setHeader("replyId", document.replyId)
                        self.write(document.document)
                    self.finish()
                    s.close()
                    return

        if self.path.startswith(b"/opt/in"):
            if self.path == b"/opt/in" or self.path == b"/opt/in/":
                archived = self.args.get(b'archived', [b'0'])[0] == b'1'
                reply_id = self.args.get(b'replyId', [None])[0]
                if archived is True or reply_id is not None:
                    limit = int(self.args.get(b'limit', [0])[0])
                    offset = int(self.args.get(b'offset', [0])[0])
                    if limit == 0:
                        limit = 500

                    s = session()
                    self.setResponseCode(http.OK)
                    self.setHeader("Content-Type", "text/xml;charset=utf-8")
                    now = datetime.now()
                    stamp = mktime(now.timetuple())
                    self.setHeader("Date", format_date_time(stamp))
                    self.setHeader("Server", "UTM cached proxy")

                    top = Element('A')
                    query = s.query(UtmDocument).filter(UtmDocument.documentType == "request").order_by(
                        desc(UtmDocument.id))
                    if reply_id is not None:
                        query = query.filter(UtmDocument.replyId == reply_id)
                    if limit > 0:
                        query = query.limit(limit)
                    if offset > 0:
                        query = query.offset(offset)
                    for doc in query.all():
                        child = SubElement(top, 'url')
                        child.text = "http://%s:%s%s/%s" % (self_host, self_port, doc.utmPath, doc.id)
                        if len(doc.replyId) > 0:
                            child.set("replyId", doc.replyId)
                    ver = SubElement(top, 'ver')
                    ver.text = '1'
                    self.write(tostring(top))

                    self.finish()
                    s.close()
                    return
            else:
                pattern = re.compile(r"/opt/in/(.*)/(\d+)")
                path_array = pattern.findall(str(self.path))

                if self.method == b'GET' and len(path_array) == 1 and len(path_array[0]) == 2:
                    s = session()
                    self.setResponseCode(http.OK)
                    self.setHeader("Content-Type", "text/xml;charset=utf-8")
                    now = datetime.now()
                    stamp = mktime(now.timetuple())
                    self.setHeader("Date", format_date_time(stamp))
                    self.setHeader("Server", "UTM cached proxy")

                    query = s.query(UtmDocument).filter(UtmDocument.id == path_array[0][1],
                                                        UtmDocument.documentType == 'request')
                    if query.count() == 0:
                        self.setResponseCode(404)
                        self.finish()
                        s.close()
                        return
                    document = query.one()

                    self.setHeader("replyId", document.replyId)
                    self.write(document.document.decode('utf-8').encode('iso-8859-1'))

                    self.finish()
                    s.close()
                    return

        # иначе - перенаправляем запрос к реальному УТМ
        self.setHost(bytes(utmHost, "utf-8"), utmPort)

        self.content.seek(0, 0)
        postData = self.content.read()
        if self.method == b'POST':
            # сохраняем запрос в базу
            try:
                stream = io.BytesIO()
                stream.write(postData)
                stream.seek(0)
                content_type = self.getHeader("content-type")
                boundary = content_type[content_type.find("boundary=")+9:]
                p = mp.MultipartParser(stream, boundary)
                parts = p.parts()
                self.document = parts[0].value.encode('utf-8')
            except:
                pass

        factory = ProxyClientFactory(self.method, self.uri, postData,
                                     self.requestHeaders.getAllRawHeaders(),
                                     self)
        self.reactor.connectTCP(utmHost, utmPort, factory)

    def processResponse(self, data):
        try:
            xml_root = ET.fromstring(data.decode("utf-8"))
            items = xml_root.findall('url')
            replyId = None
            for item in items:
                replyId = item.text
            if replyId is not None and self.document is not None:
                s = session()
                doc = UtmDocument(utmPath=self.path, document=self.document, documentType="request", replyId=replyId)
                s.add(doc)
                s.commit()
                s.close()
        except:
            pass
        return data


class TransparentProxy(http.HTTPChannel):
    requestFactory = ProxyRequest


class ProxyFactory(http.HTTPFactory):
    protocol = TransparentProxy


class DownloadThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.stop = False

    def periodic(self, s, interval, action, args=()):
        if not self.stop:
            s.enter(interval, 1, self.periodic, (s, interval, action, args))
            try:
                action(*args)
            except:
                log.msg("Failed to download documents")

    def run(self):
        log.msg("Starting download thread")
        scheduler = sched.scheduler(time.time, time.sleep)
        self.periodic(scheduler, 30, self.download_documents)
        scheduler.run(True)

    def download_documents(self):
        s = session()
        xml_root = ET.parse(urlopen("http://%s:%s/opt/out" % (utmHost, utmPort))).getroot()
        items = xml_root.findall('url')
        pattern = re.compile(r".*/(.*)/(.*)")
        for item in items:
            replyId = item.get("replyId", "")
            documentUrl = item.text;
            # log.msg("Downloading document: %s" % documentUrl)
            patharray = pattern.findall(documentUrl)
            documentType = patharray[0][0]
            originalId = patharray[0][1]

            if s.query(UtmDocument).filter(UtmDocument.replyId == replyId,
                                           UtmDocument.originalId == originalId).count() > 0:
                # log.msg("Already in database: %s" % documentUrl)
                continue

            documentData = bytes(urlopen(documentUrl).read())

            documentDbObject = UtmDocument(utmPath=documentUrl,
                                           replyId=replyId,
                                           document=documentData,
                                           documentType=documentType,
                                           originalId=originalId)
            s.add(documentDbObject)
            try:
                s.commit()
                if deleteDocuments:
                    opener = build_opener(HTTPHandler)
                    request = Request(documentUrl)
                    request.get_method = lambda: 'DELETE'
                    url = opener.open(request)
                    url.close()
            except:
                log.msg("Failed to add document to database")
                s.rollback()

        s.close()


class CleanupThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.stop = False

    def periodic(self, s, interval, action, args=()):
        if not self.stop:
            s.enter(interval, 1, self.periodic, (s, interval, action, args))
            try:
                action(*args)
            except Exception as e:
                log.msg("Failed to delete old documents: %s" % sys.exc_info()[0])

    def run(self):
        log.msg("Starting cleanup thread")
        scheduler = sched.scheduler(time.time, time.sleep)
        self.periodic(scheduler, 3600, self.cleanup_documents)
        scheduler.run(True)

    def cleanup_documents(self):
        s = session()
        current_time = datetime.now()
        two_weeks_ago = current_time - timedelta(weeks=2)
        three_days_ago = current_time - timedelta(days=3)
        q = s.query(UtmDocument).filter(UtmDocument.documentType == 'ReplyRests',
                                        UtmDocument.archived == True)
        for doc in q.all():
            s.delete(doc)
            log.msg("Deleting document %s № %s, replyId = %s" % (doc.documentType, doc.id, doc.replyId))
        q = s.query(UtmDocument).filter(UtmDocument.documentType == 'INVENTORYREGINFO',
                                        UtmDocument.ts < three_days_ago)
        for doc in q.all():
            s.delete(doc)
            log.msg("Deleting document %s № %s, replyId = %s" % (doc.documentType, doc.id, doc.replyId))
        q = s.query(UtmDocument).filter(UtmDocument.ts < two_weeks_ago,
                                        UtmDocument.archived == True)
        for doc in q.all():
            s.delete(doc)
            log.msg("Deleting document %s № %s, replyId = %s" % (doc.documentType, doc.id, doc.replyId))
        q = s.query(UtmDocument).filter(UtmDocument.ts < two_weeks_ago,
                                        UtmDocument.documentType == 'request')
        for doc in q.all():
            s.delete(doc)
            log.msg("Deleting request %s № %s, replyId = %s" % (doc.utmPath, doc.id, doc.replyId))
        try:
            s.commit()
        except:
            log.msg("Failed to delete documents from database: %s" % sys.exc_info()[0])
            s.rollback()
        s.close()


class ReactorThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        log.msg("Starting reactor service")
        reactor.listenTCP(proxyPort, ProxyFactory())
        reactor.run(installSignalHandlers=0)


class AppServerSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "UtmProxy"
    _svc_display_name_ = "UTM proxy service"
    _svc_description_ = "UTM proxy download all UTM documents to local DB and don't allow UTM to delete them"
#    _svc_deps_ = ["MSSQLSERVER"]

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.hWaitResume = win32event.CreateEvent(None, 0, 0, None)
        self.timeout = 10000
        self.resumeTimeout = 1000
        self._paused = False

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STOPPED,
                              (self._svc_name_, ''))

    def SvcPause(self):
        self.ReportServiceStatus(win32service.SERVICE_PAUSE_PENDING)
        self._paused = True
        self.ReportServiceStatus(win32service.SERVICE_PAUSED)
        servicemanager.LogInfoMsg("The %s service has paused." % (self._svc_name_,))

    def SvcContinue(self):
        self.ReportServiceStatus(win32service.SERVICE_CONTINUE_PENDING)
        win32event.SetEvent(self.hWaitResume)
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        servicemanager.LogInfoMsg("The %s service has resumed." % (self._svc_name_,))

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("The %s service is running." % (self._svc_name_,))
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        servicemanager.LogInfoMsg("UTM proxy running")

        download_thread = DownloadThread(1, "DownloadThread", 1)
        download_thread.start()

        reactor_thread = ReactorThread(2, "ReactorThread", 1)
        reactor_thread.start()

        cleanup_thread = CleanupThread(3, "CleanupThread", 1)
        cleanup_thread.start()

        while True:
            rc = win32event.WaitForSingleObject(self.hWaitStop, self.timeout)
            if rc == win32event.WAIT_OBJECT_0:
                servicemanager.LogInfoMsg("Bye!")
                download_thread.stop = True
                reactor.stop()
                session.remove()
                break

            if self._paused:
                servicemanager.LogInfoMsg("I'm paused... Keep waiting...")

            while self._paused:
                rc = win32event.WaitForSingleObject(self.hWaitResume, self.resumeTimeout)
                if rc == win32event.WAIT_OBJECT_0:
                    self._paused = False
                    servicemanager.LogInfoMsg("Yeah! Let's continue!")
                    break

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)
