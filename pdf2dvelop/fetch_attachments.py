import email
from email.header import decode_header
import logging
import imaplib
import msal
import os
import sys
from dotenv import dotenv_values
import random
import string
import graypy
from datetime import datetime


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def kill_umlaut(string_with_umlaut: str):
    string_with_umlaut = string_with_umlaut.replace('Ä', 'Ae')
    string_with_umlaut = string_with_umlaut.replace('Ö', 'Oe')
    string_with_umlaut = string_with_umlaut.replace('Ü', 'Ue')
    string_with_umlaut = string_with_umlaut.replace('ß', 'ss')
    string_with_umlaut = string_with_umlaut.replace('ä', 'ae')
    string_with_umlaut = string_with_umlaut.replace('ö', 'oe')
    string_with_umlaut = string_with_umlaut.replace('ü', 'ue')
    return string_with_umlaut


settings = dotenv_values(os.path.join(os.path.abspath(os.path.dirname(__file__)), '.env'))
log_method = settings.get("log_method", "file").lower()
log_level = settings.get("log_level", "info").lower()
logger = logging.getLogger(__name__)
log_levels = {'debug': 10, 'info': 20, 'warning': 30, 'error': 40, 'critical': 50}
logger.setLevel(log_levels.get(log_level, 20))
sys.excepthook = handle_unhandled_exception

if log_method == "file":
    log_file_name = f"cron_fetchmails_{datetime.now().strftime('%Y_%m_%d')}.log"
    log_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "log", log_file_name)

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        filename=log_path,
                        filemode='a')

elif log_method == "graylog":
    graylog_host = settings.get("graylog_host", "127.0.0.1")
    graylog_port = int(settings.get("graylog_port", 12201))
    handler = graypy.GELFUDPHandler(graylog_host, graylog_port)
    logger.addHandler(handler)

logger.info("fetch_attachments gestartet.")


def download_attachment(ppart, tempfolder: str) -> str | None:
    if not ppart.get_filename():
        return None
    efilename, encoding = decode_header(ppart.get_filename())[0]
    if encoding is not None:
        orig_filename = efilename.decode(encoding)
    else:
        orig_filename = ppart.get_filename()

    orig_filebase = os.path.splitext(orig_filename)[0]
    orig_extension = os.path.splitext(orig_filename)[1]
    dest_filename = "{}_{}{}".format(orig_filebase, get_random_string(6), orig_extension)
    dest_filepath = os.path.join(tempfolder, dest_filename)

    if os.path.isdir(tempfolder):
        open(dest_filepath, "wb").write(ppart.get_payload(decode=True))

    return dest_filepath


def obtain_header(pmsg):
    # decode the email subject
    subject, encoding = decode_header(pmsg["Subject"])[0]
    if isinstance(subject, bytes):
        subject = subject.decode(encoding)

    # decode email sender
    sender, encoding = decode_header(pmsg.get("From"))[0]
    if isinstance(sender, bytes):
        sender = sender.decode(encoding)

    return subject, sender


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for _ in range(length))
    return result_str


def generate_oauth2_string(username, access_token):
    auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
    return auth_string


# Sonstige Einstellungen
move_error_mails_to = settings.get("move_error_mails_to")
mail_backup_to = settings.get("mail_backup_to")
if mail_backup_to is None or len(mail_backup_to) == 0:
    mail_backup_to = "BACKUP"

config = {
    "authority": settings.get("authority"),
    "client_id": settings.get("client_id"),
    "scope": [settings.get("scope")],
    "secret": settings.get("secret"),
    "tenant-id": settings.get("tenant_id")
}
appcontext = msal.ConfidentialClientApplication(config['client_id'], authority=config['authority'],
                                                client_credential=config['secret'])
result = appcontext.acquire_token_silent(config["scope"], account=None)

if not result:
    result = appcontext.acquire_token_for_client(scopes=config["scope"])

if "access_token" in result:
    user = settings.get("mailuser")
    server = settings.get("mailserver")
    cache_contractors = settings.get("cache_contractors")
    cache_use_units = settings.get("cache_use_units")
    mailbox = imaplib.IMAP4_SSL(server)
    mailbox.debug = 4
    mailbox.authenticate('XOAUTH2', lambda x: generate_oauth2_string(user, result['access_token']).encode('utf-8'))

    mailbox.select('INBOX')
    # Alle E-Mails im Postfach abrufen
    _, message_ids = mailbox.search(None, 'ALL')
    message_ids = message_ids[0].split()
    logger.info(f"Mails gefunden: {len(message_ids)}")

    # Relative temp-dir
    ptempdir = settings.get("att_dlpath")
    workcnt = 0
    attfiles = []

    # Durch jede E-Mail iterieren
    try:
        for message_id in message_ids:
            workcnt += 1
            mail_has_attachment = False
            # E-Mail abrufen
            _, email_data = mailbox.fetch(message_id, '(RFC822)')
            for response in email_data:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])

                    msgsubject, msgsender = obtain_header(msg)
                    print("Subject:", msgsubject)

                    msgfullbody = ""

                    if msg.is_multipart():
                        for part in msg.walk():
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))

                            if "attachment" in content_disposition or "inline" in content_disposition:
                                dlfilepath = download_attachment(part, ptempdir)
                                if dlfilepath is not None:
                                    mail_has_attachment = True
                                    print(dlfilepath)
                                    if dlfilepath in attfiles:
                                        print(f"{dlfilepath} doppelt!")
                                    else:
                                        attfiles.append(dlfilepath)
                                    print(len(attfiles))

                    # E-Mail in den "BACKUP"-Ordner verschieben
                    mailbox.copy(message_id, mail_backup_to)

                    # Ursprüngliche E-Mail im Postfach löschen
                    mailbox.store(message_id, '+FLAGS', '\\Deleted')

    except imaplib.IMAP4.abort as e:
        logger.error(f"IMAP4-Fehler: {e.args}")

    # Änderungen am Postfach übernehmen und Verbindung trennen
    mailbox.expunge()
    mailbox.close()
    mailbox.logout()
    print(workcnt)

else:
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))  # You may need this when reporting a bug
