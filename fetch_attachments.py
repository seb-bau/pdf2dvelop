import email
from email.header import decode_header
from email.utils import parseaddr
import logging
import imaplib
import msal
import os
import sys
from dotenv import dotenv_values
import random
import string
import graypy
from bs4 import BeautifulSoup
from wowipy.wowipy import WowiPy, TicketAssignment, CommunicationCatalog, Contractor, UseUnit
from dvelopdmspy.dvelopdmspy import DvelopDmsPy
from datetime import datetime
import re


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


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

logger.info("cron_fetchmails gestartet.")


def download_attachment(ppart, tempfolder: str) -> str | None:
    orig_filename = ppart.get_filename()
    if not orig_filename:
        return None

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


def get_fallback_assignment(psettings: dict, wowicon: WowiPy, commcat: CommunicationCatalog) -> TicketAssignment | None:
    fallback_assignment_entity_type = psettings.get("fallback_assignment_entity_type")
    fallback_entity_idnum = psettings.get("fallback_entity_idnum")
    if fallback_assignment_entity_type is None or len(fallback_assignment_entity_type) == 0:
        return None
    if fallback_entity_idnum is None or len(fallback_entity_idnum) == 0:
        return None

    t_assid = 0
    tick_ass_type = commcat.ticket_assignment_entity_id.get(fallback_assignment_entity_type)

    if fallback_assignment_entity_type.lower() == "vertrag":
        found = wowicon.get_license_agreements(license_agreement_idnum=fallback_entity_idnum)
        if found is not None and len(found) > 0:
            t_assid = found[0].id_

    if t_assid > 0:
        print(f"Fallback: {tick_ass_type} / {t_assid}")
        return TicketAssignment(tick_ass_type, t_assid)
    else:
        return None


def build_assignments(content_fields: dict, wowicon: WowiPy, commcat: CommunicationCatalog,
                      assign_person: bool = False, main_assignment_type: str = "Vertrag",
                      fallback_assignment: TicketAssignment = None, always_check_address: bool = False,
                      primary_criteria: str = "mail"):
    print(f"assign_person: {assign_person}")
    print(f"main_ass_type: {main_assignment_type}")
    if len(content_fields.get("mail")) == 0 and len(content_fields.get("name")) == 0:
        return None, None

    assignments = []
    main_assignment = None
    person_assigned = False
    # Contractors anhand der primären Suchmethode suchen
    print(f"Primary Criteria: {primary_criteria}")

    if primary_criteria.lower() == "mail":
        found = wowicon.search_contractor(search_email=content_fields.get("mail"), allow_duplicates=True)

        # Bei der Mailadresse muss nicht zwingend die Adresse geprüft werden
        check_addr = always_check_address
    else:
        found = wowicon.search_contractor(search_name=content_fields.get("name"), allow_duplicates=True)
        print(f"Check Name {content_fields.get('name')} --> {len(found)}")

        # Nur der Name kann durchaus mehrfach vorkommen
        check_addr = True

    # Wenn mit der primären Methode nichts gefunden wurde: Mit weiteren Methoden weiter machen
    if len(found) == 0:
        if primary_criteria.lower() == "mail":
            found = wowicon.search_contractor(search_name=content_fields.get("name"), allow_duplicates=True)
            check_addr = True
        else:
            found = wowicon.search_contractor(search_email=content_fields.get("mail"), allow_duplicates=True)
            check_addr = always_check_address

    entry: Contractor
    for entry in found:
        if check_addr:
            if entry.person is not None and entry.person.addresses is not None and len(entry.person.addresses) > 0:
                if entry.person.addresses[0].street_complete != content_fields.get("address"):
                    print(f'{entry.person.addresses[0].street_complete} != {content_fields.get("address")}')
                    continue
            else:
                continue
        if entry.end_of_contract is not None:
            if type(entry.end_of_contract) == str:
                eoc = datetime.strptime(str(entry.end_of_contract), "%Y-%m-%d")
            else:
                eoc = entry.end_of_contract
            if datetime.today() > eoc:
                print(f"{eoc} liegt nach {datetime.today()}")
                continue
        if assign_person and not person_assigned:
            tick_ass_type = commcat.ticket_assignment_entity_id.get("Person")
            tick_ass_id = entry.person.id_
            person_assignment = TicketAssignment(assignment_entity_id=tick_ass_type, entity_id=tick_ass_id)
            print(f"Type: {tick_ass_type}")
            if main_assignment_type.lower() == "person":
                main_assignment = person_assignment
            else:
                assignments.append(person_assignment)
            person_assigned = True

        tick_ass_type = commcat.ticket_assignment_entity_id.get("Vertrag")
        tick_ass_id = entry.license_agreement_id
        contract_assignment = TicketAssignment(assignment_entity_id=tick_ass_type, entity_id=tick_ass_id)

        if main_assignment_type.lower() == "vertrag" and main_assignment is None:
            use_unit: UseUnit
            use_unit = wowicon.get_use_units(use_unit_idnum=entry.use_unit.use_unit_number, use_cache=True)[0]
            if use_unit.current_use_unit_type.use_unit_usage_type.name == "Wohnung":
                main_assignment = contract_assignment
        else:
            assignments.append(contract_assignment)

    if main_assignment is None and len(assignments) > 0:
        main_assignment = assignments[0]

    if main_assignment is None and fallback_assignment is not None:
        main_assignment = fallback_assignment

    print("Kontrolle")
    if main_assignment is not None:
        print(f"Main Type: {main_assignment.assignment_entity_id}")
    else:
        print("Main Type is NONE")
    for kirsche in assignments:
        print(f"Kirsche Type: {kirsche.assignment_entity_id}")

    return main_assignment, assignments


def detect_content_fields(mail_from: str, mail_subject: str, mail_name: str, mail_body: str, psettings: dict) -> dict:
    print(f"Mail-Body: {mail_body}")
    regex_mail = psettings.get("content_mail")
    regex_name = psettings.get("content_name")
    regex_phone = psettings.get("content_phone")
    regex_address = psettings.get("content_address")
    regex_city = psettings.get("content_city")
    regex_postcode = psettings.get("content_postcode")
    regex_body = psettings.get("content_body")

    strip_name = psettings.get("strip_name")

    field_mail = field_name = field_phone = field_address = field_city = field_postcode = field_body = ""
    field_subject = mail_subject

    if regex_mail is None or len(regex_mail) == 0:
        # Wenn die Mailadresse nicht im Body der Mail steht verwenden wir die ursprüngliche Absenderadresse
        field_mail = mail_from
    else:
        match = re.search(regex_mail, mail_body)
        if match is not None:
            field_mail = match.group(1)

    if regex_name is None or len(regex_name) == 0:
        # Wenn der Name nicht im Body der Mail steht verwenden wir den Absendernamen
        field_name = mail_name
    else:
        match = re.search(regex_name, mail_body)
        if match is not None:
            field_name = match.group(1)
            # Bugfix: Zwei Leerzeichen im Namen
            old = "  "
            new = " "
            field_name = str(field_name).replace(old, new)

    if regex_phone is not None and len(regex_phone) > 0:
        match = re.search(regex_phone, mail_body)
        if match is not None:
            field_phone = match.group(1)

    if regex_address is not None and len(regex_address) > 0:
        match = re.search(regex_address, mail_body)
        if match is not None:
            field_address = match.group(1)

    if regex_city is not None and len(regex_city) > 0:
        match = re.search(regex_city, mail_body)
        if match is not None:
            field_city = match.group(1)

    if regex_postcode is not None and len(regex_postcode) > 0:
        match = re.search(regex_postcode, mail_body)
        if match is not None:
            field_postcode = match.group(1)

    if regex_body is None or len(regex_body) == 0:
        # Wenn der Ganze Inhalt der Mail als Ticketinhalt verwendet werden soll
        field_body = mail_body
    else:
        match = re.search(regex_body, mail_body)
        if match is not None:
            field_body = match.group(1)

    # String-Strip
    if strip_name is not None and len(strip_name) > 0:
        strip_entries = strip_name.split("|")
        for strip_entry in strip_entries:
            field_name = field_name.replace(strip_entry, "")

    return {
        'mail': field_mail,
        'name': field_name,
        'subject': field_subject,
        'body': field_body,
        'address': field_address,
        'city': field_city,
        'postcode': field_postcode,
        'phone': field_phone
    }


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for _ in range(length))
    return result_str


def generate_oauth2_string(username, access_token):
    auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
    return auth_string


def process_mail(mail_subject: str, mail_body: str, mail_from_name: str, mail_from_addr: str,
                 wowicon: WowiPy, pwowi_ticket_source: str, commcat: CommunicationCatalog,
                 psettings: dict, fallback_assignment: TicketAssignment = None,
                 attcount: int = 0) -> dict | None:
    tick_source_id = commcat.ticket_source_id.get(pwowi_ticket_source)

    # Womit wollen wir das Ticket verbinden?
    # Sinnvolle Möglichkeiten: Vertrag oder Person
    # Alle Verträge? Nur Wohnraum?

    # HIER WEITERMACHEN
    # Suchmöglichkeiten für die verschiedenen Felder in env platzieren. Ggf mit RegEx-Suchstrings?
    # UNd auch einen Adressfilter einbauen
    field_dict = detect_content_fields(mail_from=mail_from_addr,
                                       mail_name=mail_from_name,
                                       mail_body=mail_body,
                                       psettings=psettings,
                                       mail_subject=mail_subject)
    print(field_dict)
    set_assign_person = psettings.get("assign_person")
    set_main_ass = psettings.get("main_assignment")
    always_check_addr_str = psettings.get("always_check_address")
    t_primary_criteria = psettings.get("primary_criteria")
    if always_check_addr_str is None or always_check_addr_str.lower() != "true":
        t_always_check_addr = False
    else:
        t_always_check_addr = True
    if set_main_ass is None or len(set_main_ass) == 0:
        set_main_ass = "Vertrag"

    if set_assign_person.lower() == "true":
        assign_person_bool = True
    else:
        assign_person_bool = False

    main_ass, ass_list = build_assignments(field_dict,
                                           wowicon=wowicon,
                                           commcat=commcat,
                                           assign_person=assign_person_bool,
                                           main_assignment_type=set_main_ass,
                                           fallback_assignment=fallback_assignment,
                                           always_check_address=t_always_check_addr,
                                           primary_criteria=t_primary_criteria)
    if main_ass is None:
        return None

    # Anhang-Schriftzug hinzufügen
    if attcount > 0:
        add_att_msg = psettings.get("add_att_msg")
        if add_att_msg is not None and len(add_att_msg) > 0:
            field_dict["body"] = f"{add_att_msg}\n\n{field_dict.get('body')}"

    print(f"Create Ticket. Main: {main_ass.assignment_entity_id} / {main_ass.entity_id}")

    tresult = wowicon.create_ticket(subject=field_dict.get("subject"),
                                    content=field_dict.get("body"),
                                    source_id=tick_source_id,
                                    main_assignment=main_ass,
                                    assignments=ass_list)
    if tresult.status_code == 201:
        return tresult.data
    else:
        return None


def process_attachments(attachments: list, ticket_id: id, dmscon: DvelopDmsPy, dmsuploadcat: str):
    if len(attachments) == 0:
        return

    dms_cat_id = dmscon.add_category(dmsuploadcat)
    sprops = dmscon.add_upload_property("Ticket-ID", ticket_id)

    for tatt in attachments:
        doc_id = dmscon.archive_file(tatt, dms_cat_id[0], sprops)
        print(f"DOK-ID: {doc_id}")


def mail_address_matches(pallowed_pattern: list, pmail_address: str) -> bool:
    if pallowed_pattern is None:
        return True

    for pattern_entry in pallowed_pattern:
        if re.match(pmail_address.lower(), pattern_entry):
            return True

    return False


# Wowiport settings
wowi_ticket_source = settings.get("wowi_ticket_source")
wowi_host = settings.get("wowi_host")
wowi_user = settings.get("wowi_user")
wowi_pass = settings.get("wowi_pass")
wowi_key = settings.get("wowi_key")

# Dvelop settings
dms_host = settings.get("dvelop_host")
dms_key = settings.get("dvelop_key")
dms_repo = settings.get("dvelop_repo")
dms_cat = settings.get("dvelop_cat_name")

# Sonstige Einstellungen
move_error_mails_to = settings.get("move_error_mails_to")
mail_backup_to = settings.get("mail_backup_to")
if mail_backup_to is None or len(mail_backup_to) == 0:
    mail_backup_to = "BACKUP"
ignore_not_matched_sender_str = settings.get("ignore_not_matched_sender")
if ignore_not_matched_sender_str is None or ignore_not_matched_sender_str.lower() != "true":
    ignore_unmatched_sender = False
else:
    ignore_unmatched_sender = True

# Mailadressfilter
senders_allowed = None
tsenders_allowed = settings.get("senders_allowed")
if tsenders_allowed is not None and len(tsenders_allowed) > 0:
    splsenders = tsenders_allowed.split('\n')
    senders_allowed = []
    for part in splsenders:
        if len(part.strip()) > 0:
            senders_allowed.append(part.strip())
    if len(senders_allowed) == 0:
        senders_allowed = None

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
    logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
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
    wowi = None
    dms = None
    catalogs = None
    tfallback = None
    if len(message_ids) > 0:
        # Verbindung zu OPENWOWI und dvelop aufbauem
        wowi = WowiPy(hostname=wowi_host, user=wowi_user, password=wowi_pass, api_key=wowi_key)
        catalogs = wowi.get_communication_catalogs()
        wowi.cache_from_disk(cache_type=wowi.CACHE_CONTRACTORS, file_name=cache_contractors)
        wowi.cache_from_disk(cache_type=wowi.CACHE_USE_UNITS, file_name=cache_use_units)
        if dms_repo is not None and len(dms_repo) == 0:
            dms_repo = None
        dms = DvelopDmsPy(hostname=dms_host, api_key=dms_key, repository=dms_repo)

        # Fallback-Assignment auslesen
        tfallback = get_fallback_assignment(psettings=settings,
                                            wowicon=wowi,
                                            commcat=catalogs)

    logger.info(f"Mails gefunden: {len(message_ids)}")

    # Relative temp-dir
    ptempdir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "temp")

    body_types = ['text/plan', 'text/html']

    # Durch jede E-Mail iterieren
    try:
        for message_id in message_ids:
            # E-Mail abrufen
            _, email_data = mailbox.fetch(message_id, '(RFC822)')
            for response in email_data:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])

                    msgsubject, msgsender = obtain_header(msg)
                    print("Subject:", msgsubject)
                    msgsenderparts = parseaddr(msgsender)
                    msgsendername = msgsenderparts[0]
                    msgsenderaddr = msgsenderparts[1]
                    print("Name: ", msgsendername)
                    print("Addr: ", msgsenderaddr)
                    attfiles = []
                    msgfullbody = ""
                    if not mail_address_matches(senders_allowed, msgsenderaddr):
                        print(f"Adresse {msgsenderaddr} entspricht keinem Senderfilter")
                        if not ignore_unmatched_sender:
                            if move_error_mails_to is not None and len(move_error_mails_to) > 0:
                                mailbox.copy(message_id, move_error_mails_to)
                                mailbox.store(message_id, '+FLAGS', '\\Deleted')
                        continue

                    if msg.is_multipart():
                        for part in msg.walk():
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))
                            print(f"Content-Disposition: {content_disposition}")
                            print(f"Content-Type: {content_type}")
                            body = None
                            try:
                                body = part.get_payload(decode=True).decode()
                            except UnicodeDecodeError:
                                body = part.get_payload(decode=True)
                            except AttributeError:
                                pass

                            if content_type in body_types and "attachment" not in content_disposition:
                                if content_type == "text/html":
                                    soup = BeautifulSoup(body, features="html.parser")
                                    body = soup.get_text(separator='\n', strip=True)
                                msgfullbody = f"{msgfullbody}{body}"
                            elif "attachment" in content_disposition or "inline" in content_disposition:
                                dlfilepath = download_attachment(part, ptempdir)
                                if dlfilepath is not None:
                                    attfiles.append(dlfilepath)
                    else:
                        content_type = msg.get_content_type()
                        body = msg.get_payload(decode=True).decode()
                        if content_type in body_types:
                            if content_type == "text/html":
                                soup = BeautifulSoup(body, features="html.parser")
                                body = soup.get_text(separator='\n', strip=True)
                            msgfullbody = f"{msgfullbody}{body}"

                    crresult = process_mail(mail_subject=msgsubject,
                                            mail_body=msgfullbody,
                                            mail_from_name=msgsendername,
                                            mail_from_addr=msgsenderaddr,
                                            wowicon=wowi,
                                            pwowi_ticket_source=wowi_ticket_source,
                                            commcat=catalogs,
                                            psettings=settings,
                                            fallback_assignment=tfallback,
                                            attcount=len(attfiles))
                    if crresult is not None:
                        newticket_id = crresult.get("Id")
                        newticket_idnum = crresult.get("IdNum")
                        print(newticket_idnum)

                        if len(attfiles) > 0:
                            process_attachments(attachments=attfiles, ticket_id=newticket_id,
                                                dmscon=dms, dmsuploadcat=dms_cat)

                        # E-Mail in den "BACKUP"-Ordner verschieben
                        mailbox.copy(message_id, mail_backup_to)

                        # Ursprüngliche E-Mail im Postfach löschen
                        mailbox.store(message_id, '+FLAGS', '\\Deleted')
                    else:
                        if move_error_mails_to is not None and len(move_error_mails_to) > 0:
                            mailbox.copy(message_id, move_error_mails_to)
                            mailbox.store(message_id, '+FLAGS', '\\Deleted')
    except imaplib.IMAP4.abort as e:
        logger.error(f"IMAP4-Fehler: {e.args}")

    # Änderungen am Postfach übernehmen und Verbindung trennen
    mailbox.expunge()
    mailbox.close()
    mailbox.logout()

else:
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))  # You may need this when reporting a bug
