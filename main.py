from os import getenv
import requests
from datetime import datetime, timedelta
import logging
from time import sleep

import pandas as pd
import yaml

from resources.supportFunctions import sqlConnection, secrets
from resources.tables import Tables
from resources.getToken import get_token as getToken


app_env = getenv("RUNTIME_ENV", "local")

testing = True



def get_resp(url, headers, expandCol, expandColSelect, cols, pagination=None, next=None, addpars={}):

    batch_size = 5000 #max 20000

    if not cols:
        print('no columns selected')
    # colsExpand = [f"{expandCol}/{col}" for col in expandColSelect]

    pars = {
        "$filter": {f"{addpars.get('$filter', [])}" if addpars.get('$filter', []) else ''},
        "$expand": expandCol + (f"($select={','.join(expandColSelect)})") if expandCol else None,
        "$select": ','.join(cols),
    } | addpars

    print(pars)
    
    data = []
    page = 1
    while True:
        if page > 100:
            break
        print(f'Getting page {page}') if app_env == 'local' else None
        cursor = {
            '$top': batch_size,
            '$skip': ((page - 1) * batch_size)
        }


        for attempt in range(3):
            try:
                resp = requests.get(url, headers=headers, params=cursor|pars, timeout=120)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                logging.warning(f"Retry {attempt+1}: {e}")
                sleep(4 * (attempt + 1))
        # resp = requests.get(url, headers=headers, params=cursor|addpars, timeout=120)
        if not resp:
            raise Exception(f"Error getting data. Response")
        
        if resp.status_code != 200:
            raise Exception(f"Error getting data. Response: {resp.json()}")
        
        resp = resp.json()['value']
        if len(resp) > 0:
            data += resp
            page += 1
        else:
            break
        if not pagination:
            break
    return data        

def makeFilter(syncLog, tableName, field, addpars=None):
    # print(addpars)
    # print(f'{addpars} and' if addpars else '')

    if syncLog.get(tableName):
        lastSyncDate = syncLog[tableName] - timedelta(minutes=3)
    else: 
        lastSyncDate = None

    if addpars and lastSyncDate and field:
        return {'$filter': f"""{addpars} and {field} ge {lastSyncDate.strftime("%Y-%m-%dT%H:%M:%SZ")}"""}
    elif addpars:
        return {'$filter': f"{addpars}"}
    elif lastSyncDate and field:
        return {'$filter': f"{field} ge {lastSyncDate.strftime("%Y-%m-%dT%H:%M:%SZ")}"}
    else:
        return {}


# def documentHeaders(headers):

#     """GET DOCUMENT HEADERS"""

#     base_url = f"""https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/{environment}/api/boltrics/boltrics/v1.0/wmsDocumentHeaders"""
#     filter = f"""arrivedDate le {(datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")} and arrivedDate ge {(datetime.now()).strftime("%Y-%m-%dT%H:%M:%SZ")}"""


#     params = {
#         "$filter": filter
#     }

#     response = requests.get(base_url, headers=headers, params=params).json()
#     # print(response)
#     df = pd.DataFrame(response['value'])
#     cols = ["documentType", "no", "sellToCustomerNo", "billToCustomerNo", "dossierNo", "buildingCode", "billOfLadingType", "direction", "locationNo", "voyageNo", "timeSlotBookingNo", "timeSlotNeededCapacity", "movementType", "documentDate", "orderDate", "postingDate", "statusCode", "orderpicker", "comment", "docInfoSetID", "externalDocumentNo", "externalReference", "shippingAgentCode", "vehicleNo", "trailerContainerNo", "dockNo", "announcedDate", "announcedTime", "arrivedDate", "arrivedTime", "departedDate", "departedTime", "waitingDuration", "destination", "plannedStartDate", "plannedEndDate", "plannedStartTime", "plannedEndTime", "containerNo", "orderTypeCode", "billOfLadingNo", "senderAddressNo", "shipToAddressNo", "sendersAddressName", "shipToAddressName", "shipToAddressCity", "tripNo", "attribute01", "attribute02", "attribute03", "attribute04", "attribute05", "attribute06", "attribute07", "attribute08", "attribute09", "attribute10", "grossWeight", "netWeight", "quantity", "carrierQuantity", "shipperName"]
#     df = df[cols]

#     df = df.astype(str)
#     df.to_csv("headers.csv", index=False, sep=";")
#     return


def main():
    with open("configCustomers.yaml", 'r') as f:
        customers = (yaml.safe_load(f))['customers']

    with open("configDatabase.yaml", 'r') as f:
        configDatabase = (yaml.safe_load(f))


    tbls = [
        #Klaar meta

        #Klaar fact


        {'name': 'wmsDocumentHeaders', 'func': Tables.check, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentHeaders', 'merge': False, 'mergeKeys': ['id'], 'filter': True, 'filterField': None, 'cols': ["documentType", "no", "sellToCustomerNo", "billToCustomerNo", "dossierNo", "buildingCode", "billOfLadingType", "direction", "locationNo", "voyageNo", "timeSlotBookingNo", "timeSlotNeededCapacity", "movementType", "documentDate", "orderDate", "postingDate", "statusCode", "orderpicker", "comment", "docInfoSetID", "externalDocumentNo", "externalReference", "shippingAgentCode", "vehicleNo", "trailerContainerNo", "dockNo", "announcedDate", "announcedTime", "arrivedDate", "arrivedTime", "departedDate", "departedTime", "waitingDuration", "destination", "plannedStartDate", "plannedEndDate", "plannedStartTime", "plannedEndTime", "containerNo", "orderTypeCode", "billOfLadingNo", "senderAddressNo", "shipToAddressNo", "sendersAddressName", "shipToAddressName", "shipToAddressCity", "tripNo", "attribute01", "attribute02", "attribute03", "attribute04", "attribute05", "attribute06", "attribute07", "attribute08", "attribute09", "attribute10", "grossWeight", "netWeight", "quantity", "carrierQuantity", "shipperName"], 'expandCol': 'wmsDocumentLines', 'expandColSelect': ["documentNo", "lineNo", "sellToCustomerNo", "buyFromVendorNo", "dossierNo", "lineAmountLCY", "originalLineQty", "qtyPerCarrier", "type", "no", "itemNo", "description", "unitOfMeasureCode", "baseUnitOfMeasureCode", "quantity", "carrierQtyPosted", "grossWeight", "netWeight", "locationNo", "buildingCode", "tariffDescription", "orderTypeCode", "postingDate", "attribute02", "attribute03", "attribute04", "attribute06", "attribute09", "carrierQuantity"], 'pars': f"""arrivedDate le {(datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")} and arrivedDate ge {(datetime.now()).strftime("%Y-%m-%dT%H:%M:%SZ")}"""},


    ]


    for customer in customers:
        
        s = secrets(vaultUrl='', local=True if app_env == 'local' else False, prefix=customer['credPrefix'])

        clientSecret = s.get('ClientSecret')
        tenantId = s.get('TenantId')
        clientId = s.get('ClientId')
        environment = s.get('Environment')

        headers = {
            'Authorization': f'Bearer {getToken(tenantId, clientId, clientSecret)}',
            'Accept': 'application/json'
        }

        
        dbName = customer['database']
        sqlc = sqlConnection(customer, s.get('SqlUser'), s.get('SqlPassword'))
        sqlc.constructDatabase(configDatabase, dbName)



        # if not account['id'] == 10:
        #     continue
        base_url = f"""https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/api"""

       
        syncLogName = 'syncLog'
        synclog = dict(sqlc.execSingle(f"SELECT [tableName], MAX([date]) AS [date] FROM {dbName}.[{syncLogName}] GROUP BY [tableName]"))
        for t in tbls:
            tableName = t['name']
            print(f"Working on {tableName}") if app_env == 'local' else None
            endpoint = f'{t['urlDir']}/{t['endpoint']}'
            url = f"{base_url}/{endpoint}"

            # print((makeFilter(synclog, tableName, t['filterField'], t.get('pars', None)) if t['filter'] else {}))
            # exit()

            resp = get_resp(url, headers=headers, pagination=False if testing else True, addpars=(makeFilter(synclog, tableName, t['filterField'], t.get('pars', None)) if t['filter'] else {}), cols=t['cols'], expandCol=t['expandCol'], expandColSelect=t['expandColSelect'])
            if len(resp) == 0 and testing:
                continue
            df = t['func'](resp)
            try:
                sqlc.writeMany(df, tableName, dbName, merge=t['merge'], mergeKeys=t['mergeKeys'], syncTableName=syncLogName)
            except Exception as e:
                # df.to_csv(f"check_{tableName}.csv", index=False, sep=";")
                print(df.dtypes)
                print(f"Error in {t['name']} >>>> {e}")
                logging.exception(e)

            # df.to_csv(f"csv/check_{endpoint.split('/')[-1]}.csv", index=False, sep=";", mode='a', header=True)

        # Niet nodig
        # sqlc.writeMany(pd.DataFrame(customerEntities), "internalEntities", schemaName, None, merge=True, mergeKeys=['id'], log=False)
        sqlc.close()
    return




if __name__ == "__main__":
    main()
