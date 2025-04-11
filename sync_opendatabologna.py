#REQUESTS = {
#    "joins": {
#        "action": "ask",
#        "format": "json",
#        "query": "[[IsA::DataSetJoin]]|?LeftDataSet|?LeftColumn|?RightDataSet|?RightColumn",
#        "api_version": "3"
#    }
#}
#
#
#
#import requests
#
#DATA = {}
#
#for k in REQUESTS: 
#    DATA[k] = requests.get('http://51.15.160.236:8880/api.php', params=REQUESTS[k]).json()
#
#print(DATA)

from settings import * 


from mwclient import Site
import requests

BOLO = f"""https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets?limit=$$LIMIT$$&offset=$$OFFSET$$&timezone=UTC&include_links=true&include_app_metas=false"""
IPP = 100


def get_url(page):
    return BOLO.replace('$$LIMIT$$', str(IPP)).replace('$$OFFSET$$', str((page-1)*IPP))

def create_page(dataset):
    PAGE = f"""
{{{{#knowledgegraph:
|nodes= {{{{PAGENAME}}}}
|depth=10
|show-property-type=false
|graph-options=MediaWiki:KnowledgeGraphOptions
|property-options?Organization logo=KnowledgeGraphOptionsImage
|width=100%
|height=400px 
}}}}

[[Source::AutoImport]]

={dataset['metas']['default']['title']}=

{{{{DataSet
| name = {dataset['metas']['default']['title']}
| id = {dataset['dataset_id']}
| opendata = https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{dataset['dataset_id']}/exports/json?lang=it&timezone=UTC
| opendata_description = https://opendata.comune.bologna.it/explore/dataset/{dataset['dataset_id']}/information/
| provider = {dataset['metas']['dcat']['creator']}
| periodicity = {dataset['metas']['dcat']['accrualperiodicity']}
| description = {dataset['metas']['default']['description']}
}}}}

== Campi ==

"""
    for field in dataset['fields']:
        field['gb_s'] = ""
        if field['type'] in ["text", "file"]:
            field['gb'] = "Anagrafica"
        elif field['type'] in ["int", 'double']:
            field['gb'] = "Metric"
        elif field['type'] in ['date', 'datetime']:
            field['gb'] = "Timestamp"
            field["gb_s"] = field['annotations'].get('timeserie_precision', 'minute')
        elif 'geo' in field['type']:
            field['gb'] = "Geographic"
        # print(field)
        PAGE += f"{{{{DataSetColumn|column={field['name']}|label={field['label']}|description={field['description']}|type={field['type']}|GlassBoxType={field['gb']}|GlassBoxSubType={field['gb_s']}}}}}\n"

    PAGE += f"""


==Logs==
{{{{#ask:
[[RefersTo::{{{{PAGENAME}}}}]]
|?Timestamp
|?Result
}}}}
"""
    

    return PAGE
    

user_agent = 'MyCoolTool/0.2 (marco.montanari35@unibo.it)'
site = Site(WIKI_BASE, force_login=False, scheme=PROTOCOL, path="/",clients_useragent=user_agent, connection_options={"verify":False})
site.clientlogin(username=USER, password=PASS)

print(site)

for i in range(1,8):
    datasets = requests.get(get_url(i)).json()

    for ds in datasets['results']:
        if "Elezioni" in ds['metas']['default']['title'] or "Referendum" in ds['metas']['default']['title']:
            pass 
        else:
            page = create_page(ds)
            s = site.pages['OpenData - ' + ds['metas']['default']['title']]
            #if s.exists:
            #    print(s.can('delete'))
            #    s.delete()
            #    print('deleted', 'OpenData - ' + ds['metas']['default']['title'])    
            #if not s.exists:
            s.edit(page, 'import')
            print('imported', ds['metas']['default']['title'])    
            
    



