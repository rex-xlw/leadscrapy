# -*- coding: UTF-8 -*-
#!/usr/bin/env python

import traceback
from smtplib import SMTP
from email.MIMEText import MIMEText

smtpHost = "smtp.qq.com"
smtpPort = 587
smtpUsername = "786186923@qq.com"
smtpPassword = "0mmxlw20116464"
sender = "786186923@qq.com"

def sendEmail(to, subject, content):
    retval = 1
    if not(hasattr(to, "__iter__")):
        to = [to]
    destination = to

    text_subtype = 'plain'
    try:
        msg = MIMEText(content, text_subtype)
        msg['Subject'] = subject
        msg['From'] = sender # some SMTP servers will do this automatically, not all

        conn = SMTP(host=smtpHost, port=smtpPort)
        conn.set_debuglevel(True)
        #conn.login(smtpUsername, smtpPassword)
        try:
            if smtpUsername is not False:
                conn.ehlo()
                if smtpPort != 25:
                    conn.starttls()
                    conn.ehlo()
                if smtpUsername and smtpPassword:
                    conn.login(smtpUsername, smtpPassword)
                else:
                    print("::sendEmail > Skipping authentication information because smtpUsername: %s, smtpPassword: %s" % (smtpUsername, smtpPassword))
            conn.sendmail(sender, destination, msg.as_string())
            retval = 0
        except Exception, e:
            print("::sendEmail > Got %s %s. Showing traceback:\n%s" % (type(e), e, traceback.format_exc()))
            retval = 1
        finally:
            conn.close()

    except Exception, e:
        print("::sendEmail > Got %s %s. Showing traceback:\n%s" % (type(e), e, traceback.format_exc()))
        retval = 1
    return retval

if __name__ == "__main__":
    sendEmail("2027466229@txt.att.com", "Subject: Test", "Sent from Python")