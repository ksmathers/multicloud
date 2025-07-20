from ast import Not
from operator import le
from typing import Dict
from .context import Context
from .redshift_client import RedshiftClient
from .teams import Notification, OA_OPERATIONS, ARAD_DELTA_DATALAKE, NoteLevel

from datetime import datetime
import pandas as pd

TABLES = {
    'etgis': [
        'tline_mile', 'allstruc_landenv', 'domain_value', 'subtype_value',
        'domain_field', 't_ugconductorinfo', 't_towerinfo',
        't_tlinestructure', 't_switchdata', 't_poleinfo',
        't_ohconductorinfo', 't_insulator', 't_electricline',
        't_uglinesegment', 't_switch', 't_ohstructure', 't_ohlinesegment',
        't_splice', 't_towerstructure', 't_polestructure'
    ],
    'sap': [ 
        'project_master', 'notification_status', 'notification_tasks',
        'notification_item', 'inspections', 'text_data',
        'notification_activities', 'notification_causes',
        'notification_tasks_status', 'equipment_functional_loc_status',
        'notification_long_text', 'equipment_classification_data',
        'equipment_common_attributes', 'notification_header'
    ], 
    'td': None,
    'edgis': None,
    'outage': None
}

class AuditMonitor:
    def __init__(self, ctx : Context, origin : str, tables : dict, age_limit = 7):
        self.ctx = ctx
        self.dbc = RedshiftClient(ctx)
        self.origin = origin
        self.age_limit = age_limit
        self.all_tables = tables

    def update(self):
        notify = Notification(OA_OPERATIONS)
        message = self.watchlist(self.origin, self.all_tables, self.age_limit)
        return message
        notify.send_message(
            self.watchlist(self.origin, self.all_tables, self.age_limit))

    def watchlist(self, origin, all_tables=None, age_limit=7):
        dbc = self.dbc
        report = []
        df = dbc.sql_query(f"select * from audit_control where source_system = '{origin}' order by incoming_file_ts desc limit 100000")
        if all_tables is None:
            all_tables = list(df.entity_name.unique())
        tbl = pd.DataFrame()
        tnow = datetime.now()
        max_age = None
        max_entity = None
        max_date = None
        error_list = []
        for e in all_tables:
            last_update = df[df.entity_name == e].iloc[0]
            batch_run_id = last_update['batch_run_id']
            batch_run_status = last_update['batch_run_status']
            redshift_run_time = last_update['redshift_run_time']
            dt = tnow - redshift_run_time
            if batch_run_status != 'SUCCESS':
                error_list.append(f"ERROR: failed to load manifest for {e}")

            if max_age is None or dt.total_seconds() > max_age.total_seconds():
                max_entity = e
                max_age = dt
                max_date = redshift_run_time
            if dt.days > age_limit:
                report.append(f"{e:32s} {batch_run_id:8d} {batch_run_status:8s} {dt.days} days old")

        if max_age.days > age_limit:
            error_list.append(f"ERROR: {max_entity} last updated {max_date:%Y-%m-%d %H%M hrs}")

        if len(error_list)==0:
            report.append(f"GOOD: all {origin} tables less than {max_age.days+1} day(s) old")
        else:
            for err in error_list:
                report.append(err)
        return "\n".join(report)


def watchlist(ctx : Context, tables : dict = TABLES, dryrun_flag=False):
    audits = []
    age_limit = {
        'etgis': 7,
        'sap': 3,
        'outage': 365
    }
    for origin in tables.keys():
        if tables[origin] is not None:
            audits.append(AuditMonitor(ctx, origin, tables[origin], age_limit.get(origin,7)))

    messages = []
    for audit in audits: 
        messages.append(audit.update())
    message = "\n".join(messages)
    level = NoteLevel.INFO
    title = "audit_control table"
    if "ERROR" in message:
        level = NoteLevel.ERROR

    if dryrun_flag:
        print(f"Notification {level} would be sent to {ARAD_DELTA_DATALAKE}")
        print("Title:", title)
        print(message)
    else:
        notify = Notification(ARAD_DELTA_DATALAKE)
        notify.send_message(message, title=title, level=level)
