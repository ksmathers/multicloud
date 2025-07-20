import ssl
import json
import certifi
from urllib.request import Request, urlopen
from enum import Enum
import yaml

def json_escape(msg):
    return msg.replace('"', '\\"')

class NoteLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    NONE = -1

notification_channels = yaml.safe_load("""
channels:
""")


class Notification:
    def __init__(self, app_or_url, appid="2854", notify_lanid="k0sf", owner_lanid="jfpw", debug=False):
        """ - Sends notifications to Microsoft Teams using a webhook.

        url : can be a webhook URL or one of the following predefined webhooks:
            'dev' - development notifications
            'oa' - generic OA operations notifications
            'star-tline-de' - notifications associated with GIS ingestion
            'arad-delta-datalake' - notifications associated with Redshift ingestion
            'oa-application' - notifications associated with ETOA
        """
        if app_or_url in notification_channels:
            channel = notification_channels[app_or_url]
            url = channel['url']
            env = channel.get('env', 'test')
            appid = channel.get('appid', '0000')
            app = app_or_url
        else:
            app = "dca-aws-jupyter"
            env = "dev"
            url = app_or_url

        self.debug = debug
        self.env = env
        self.url = url
        self.component = app
        self.appid = appid
        self.notify_lanid = notify_lanid
        self.owner_lanid = owner_lanid

    def message_template_obm(self, html_body, html_title:str = "", level:NoteLevel = NoteLevel.INFO, preformat = True):
        if level == NoteLevel.DEBUG or level == NoteLevel.NONE:
            return None
        if level == NoteLevel.INFO or level == NoteLevel.WARN:
            newstate = "OK"
        else:
            newstate = "ALARM"
        environment = self.env.upper()
        notify = self.notify_lanid
        owner = self.owner_lanid
        appid = self.appid
        component = self.component

        alarm_template = {
            "AlarmName": html_title,
            "AlarmDescription": html_body,
            "NewStateValue": newstate,
            "AlertType": "AWSAlarm",
            "ResourceId": f"app-{appid}-{component}",
            "Tags": {
                "AppID": f"APP-{appid}",
                "CRIS": "Low",
                "Compliance": "None",
                "DataClassification": "Internal",
                "Environment": environment,
                "Notify": notify,
                "Owner": owner,
                "AppIDNum": appid
            }
        }
        
        if self.debug:
            print("sending: ", alarm_template)
        return alarm_template

    def message_template_teams(self, html_body, html_title:str = "", level:NoteLevel = NoteLevel.INFO, preformat = True):
        theme_colors = {
            NoteLevel.DEBUG: "43EB35",
            NoteLevel.INFO: "12354B",
            NoteLevel.WARN: "ABEB43",
            NoteLevel.ERROR: "EB4B35" 
        }
        theme_icon = {
            NoteLevel.DEBUG: "\U0001F41E",
            NoteLevel.INFO: "\u2630",
            NoteLevel.WARN: "\u26A0",
            NoteLevel.ERROR: "\U0001F6AB"
        }
        theme_color = "FFFFFF"
        if level != NoteLevel.NONE:
            html_title = theme_icon[level] + "\u00a0[" + level.name + "]\u00a0" + html_title
            theme_color = theme_colors[level]
        if preformat:
            html_body = "<pre>" + html_body + "</pre>"
        tmpl = {
            "title": html_title,
            "themeColor": theme_color,
            "text": html_body
        }
        return json.dumps(tmpl)

    def send_message_teams(self, msg, title="", level:NoteLevel=NoteLevel.INFO):
        # At least for the time being, the Teams webhook HTTPS connections bypass the 
        # PG&E SSL rewriting firewall, so we need to use certifi to get standard 
        # trusted root certificates instead of using the PG&E rootCA bundle.
        data = self.message_template_teams(msg, title, level)
        headers = { 'Content-Type': 'application/json' }
        req = Request(url=self.url, data=bytes(data,'utf8'), method="POST", headers=headers)
        ctx = ssl.create_default_context(cafile=certifi.where())
        resp = urlopen(req, context=ctx)
        if resp.getcode() != 200:
            print("Send message failed (Teams)")

    def send_message_obm(self, msg, title="", level:NoteLevel=NoteLevel.INFO):
        data = self.message_template_obm(msg, title, level)
        if data is None: return
        url = "https://opsconwsqa.cloud.pge.com:30005/bsmc/rest/events/aws"
        if not self.test_run:
            url = "https://opsconws.cloud.pge.com:30005/bsmc/rest/events/aws"
        req = Request(url=url, data=bytes(json.dumps(data),'utf8'), method="POST")
        ctx = ssl.create_default_context(cadata=obm_auth_crt)
        resp = urlopen(req, context=ctx)
        if self.debug:
            print("OBM Url is", url)
            print("Response code is ", resp.getcode())
        if resp.getcode() != 200:
            print("Send message failed (OBM)")

    def send_message(self, msg, title="", level:NoteLevel=NoteLevel.INFO):
        self.send_message_obm(msg, title, level)
        self.send_message_teams(msg, title, level)
        

DEVSECOPS_ETOA = None
OA_OPERATIONS = None
OA_DEVTEST = None
ARAD_DELTA_DATALAKE = None
STAR_TLINE_DE = None
OA_APPLICATION = None
DCA_JENKINS_BUILDS = None

obm_auth_crt = """
-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIQdH69BE1By6ZJMCGiCeDNwjANBgkqhkiG9w0BAQsFADA
WMRQwEgYDVQQDEwtQR0UgUm9vdCBDQTAeFw0xNTEwMDgyMzAwNDFaFw0zMDEwMD
gyMzAwNDFaMBYxFDASBgNVBAMTC1BHRSBSb290IENBMIIBIjANBgkqhkiG9w0BA
QEFAAOCAQ8AMIIBCgKCAQEAyUyTs6r+BRPOE4BTaUmz9hiwwvEmaisieBKv6gfI
vjEack9w62xfPbjeYMmL5Qln1RqEZtuTDNkhscwtUPdNOpN48jb88LjACDjQPX5
iplXNwygCaYmGT8cUfuWMK10dCzEE5Izc6Lq5IYgUwGlo7x6aFF09a1y+CkOkZT
bnNgVryEUmNs8/oVW/evnBZ6Z249lUnY4qmLtnwAPruh2CANRIPMjJj2EZySjWD
zjr4V+eW6+ktuMKlSLXut7Ta+dWgSSqXFHBNOAT/7c3DFksYJQAJ3hBKSvEOMtc
yjx5bjUSpIBrwRntZjMUtUvD/2tV2959u3pesEJVMyPnFLCknQIDAQABo1EwTzA
LBgNVHQ8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU6MHuXHNCuf
o8Iu1C4ei3Tj/NmgkwEAYJKwYBBAGCNxUBBAMCAQAwDQYJKoZIhvcNAQELBQADg
gEBAKX+B+MF/dT1mzMDs84OFvjcg4EIZ5rsOzQ9nJtCmb7duwj1n0+s/uwVzy3+
9b/O4Mvq4yRFJBnxkXaLK8N/iS/Jf7ojDIdU0dlfbOKW2xWZlGi3UcHaB23Upmn
+1Xu9om1pE3tnk7LANt3nQHsRxBcwt7TsElztzN3EoFckilAEjDelL/PbVQYoW5
DjVFNEFyu6BlUzmrXG1RG9yzwVazyU8YaZp5m+o6g4Cw3oR3ktVza/4cwL+E+FH
AhGI2qQVkrJOLzhYQH9N6cL2szPT3sghZlQmDIKh6hUni5Uf4Nmj9V8nPQQ0iMq
3hjT1u9sFr7Ght7A0dMMhFQOPFsxxRs=
-----END CERTIFICATE-----
"""


if __name__ == "__main__":
    import sys
    import os
    from copy import copy

    class Arglist:
        def __init__(self, args = None):
            if args is None: 
                args = copy(sys.argv[1:]) # skip executable 
            self.opts = {}
            self.args = args

        def __len__(self):
            return len(self.args)

        def _shift(self, default=None):
            arg = default
            if len(self.args)>0:
                arg = self.args[0]
                del self.args[0]
            return arg

        def shift_all(self, specials=[]):
            """ - returns the remaining arguments joined with spaces as a single string
            specials : a list of strings that need to be quoted"""
            p = [ '"' + x.replace("\"","\\\"") + '"' 
                    if any(special in x for special in specials)
                    else x 
                    for x in self.args ]
            self.args=[]
            return ' '.join(p)
            
        def shift(self, default=None):
            self.shift_opts()
            return self._shift(default)
            
        def shift_opts(self):
            while len(self.args)>0 and self.args[0].startswith('-') and self.args[0] != '-':
                opt = self._shift()
                if opt == '--': 
                    break
                if '=' in opt:
                    opt, optval = opt.split('=',1)
                else:
                    optval = True
                self.opts[opt[1:]]=optval

        def opt(self, optname, default=None):
            return self.opts.get(optname, default)

    notify = Notification(OA_OPERATIONS)
    import sys
    args = Arglist()
    args.shift_opts()
    msg = args.shift_all()
    opt_to = args.opt('to')
    print(opt_to)
    notelevel = args.opt('')
    notify.send_message(msg, )
