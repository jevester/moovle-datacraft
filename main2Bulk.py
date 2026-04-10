from os import getenv
import requests
from datetime import datetime, timedelta
import logging
from time import sleep, time

import pandas as pd
import yaml

from resources.supportFunctions import sqlConnection, secrets
from resources.tables import Tables
from resources.getToken import get_token as getToken


app_env = getenv("RUNTIME_ENV", "local")

testing = False



def get_resp(url, headers, expandCol, expandColSelect, cols, pagination=None, addpars={}, page=1):

    batch_size = 5000 #max 20000

    if not cols:
        print('no columns selected')
    # colsExpand = [f"{expandCol}/{col}" for col in expandColSelect]

    pars = {
        "$filter": addpars.get('$filter', None),
        "$expand": expandCol + (f"($select={','.join(expandColSelect)})") if expandCol else None,
        "$select": ','.join(cols),
    }

    # print(pars)
    
    data = []
    maxPages = 250
    ogPage = page
    while True:
        if page > ogPage + maxPages:
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



def main():
    with open("configCustomers.yaml", 'r') as f:
        customers = (yaml.safe_load(f))['customers']

    with open("configDatabase.yaml", 'r') as f:
        configDatabase = (yaml.safe_load(f))


    tbls = [
        # #Klaar meta
        # {'name': 'generalLedgerAccounts', 'func': Tables.generalLedgerAccounts, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'generalLedgerAccounts', 'merge': False, 'mergeKeys': ['id'], 'filter': False, 'filterField': 'modifiedDateTime', 'cols': ['id', 'no', 'no2', 'name', 'incomeBalance', 'accountCategory', 'accountSubcategoryEntryNo', 'accountSubcategoryDescript', 'debitCredit', 'accountType', 'totaling', 'balance', 'lastModifiedDateTime', 'genPostingType', 'vatProdPostingGroup'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsAddresses', 'func': Tables.wmsAddresses, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsAddresses', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['id', 'modifiedDateTime', "no", "name", "address", "city", "countryRegionName"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsBuildings', 'func': Tables.wmsBuildings, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsBuildings', 'merge': False, 'mergeKeys': ['id'], 'filter': False, 'filterField': None, 'cols': ['id', 'code', 'name', 'addressNo', 'modifiedDateTime', 'conditionSetID'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'customers', 'func': Tables.customers, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'customers', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'lastModifiedDateTime', 'cols': ['id', 'lastModifiedDateTime', "no", "name", "city", "ourAccountNo", "territoryCode", "globalDimension1Code", "globalDimension2Code", "chainName", "salespersonCode", "countryRegionCode", "comment", "blocked", "paymentMethodCode", "salesLCY", "profitLCY", "balanceDueLCY", "paymentsLCY", "invAmountsLCY", "crMemoAmountsLCY", "outstandingOrdersLCY", "outstandingInvoicesLCY", "noOfQuotes", "noOfBlanketOrders", "noOfOrders", "noOfInvoices", "noOfReturnOrders", "noOfCreditMemos", "lastInvoiced3PL"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'contacts', 'func': Tables.contacts, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'contacts', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'systemModifiedAt', 'cols': ['id', "no", "type", "name", "searchName", "city", "eMail", "companyNo", "companyName", "customerNo3PL", "vendorNo3PL", "statusCode3PL", 'systemModifiedAt'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'vendors', 'func': Tables.vendors, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'vendors', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'lastModifiedDateTime', 'cols': ['id', "no", "name", "city", "ourAccountNo", "territoryCode", "globalDimension1Code", "globalDimension2Code", "budgetedAmount", "shipmentMethodCode", "priority", "balanceLCY", "invDiscountsLCY", "pmtDiscountsLCY", "balanceDueLCY", "invAmountsLCY", "outstandingOrders", "reminderAmountsLCY", "outstandingInvoicesLCY", "noOfOrders", "noOfInvoices", "noOfReturnOrders", "noOfCreditMemos", "statusCode3PL", 'lastModifiedDateTime'], 'expandCol': None, 'expandColSelect': [], 'pars': None},


        # #Klaar fact
        # {'name': 'wmsDocumentLines', 'func': Tables.wmsDocumentLines, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentLines', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ["id", "documentType", "documentNo", "lineNo", "sellToCustomerNo", "sellToCustomerNo2", "buyFromVendorNo", "dossierNo", "carrierTypeCode", "currencyCode", "currencyFactor", "lineAmountLCY", "type", "no", "externalNo", "itemNo", "description", "description2", "baseUnitOfMeasureCode", "unitOfMeasureCode", "qtyPerUnitOfMeasure", "quantity", "quantityBase", "qtyOutstanding", "qtyBaseOutstanding", "storageChargeNo", "unitPrice", "lineAmount", "carrierQuantity", "qtyPerCarrier", "carrierQtyOutstanding", "createdDateTime", "createdUserID", "modifiedDateTime", "modifiedUserID", "orderUnitOfMeasureCode", "qtyPerOrderUnitOfMeasure", "tareWeightPerUoM", "grossWeightPerUoM", "netWeightPerUoM", "volumeWeight", "tareWeight", "grossWeight", "netWeight", "batchNo", "externalBatchNo", "grossWeightOutstanding", "netWeightOutstanding", "qtyToHandle", "customerItemNo2", "batchNo2", "storageChargeNo2", "unitOfMeasureCode2", "locationNo", "buildingCode", "customsCode", "tariffNo", "countryOfOriginCode", "countryPurchasedCode", "customsValue", "customsValuePer", "ncTsDocumentNo", "additionalDocumentNo", "placeOfCertificateDelivery", "destination", "declarationDocumentType", "customsCurrencyCode", "containerNo", "vesselNo", "itemHandling", "orderTypeCode", "entrepotStorage", "commentText", "externalDocumentNo", "shortcutDimension1Code", "shortcutDimension2Code", "postingDate", "invoiceType", "invoiceNo", "invoiceDate", "purchInvoiceNo", "purchInvoiceDate", "reservationPosted", "agreementType", "agreementNo", "agreementLineNo", "agreementDetailLineNo", "agreementDetailLineType", "senderAddressNo", "isActivity", "shiptoAddressNo", "activityDate", "activityTime", "customsIssueDate", "declarationDocumentNo", "attribute01", "attribute02", "attribute03", "attribute04", "attribute05", "sellToCustomerName", "buyFromVendorName", "qtyCreated", "qtyPosted", "qtyBaseCreated", "qtyBasePosted", "carrierQtyCreated", "carrierQtyPosted", "defaultNetWeightPerUOM", "grossWeightCreated", "netWeightCreated", "grossWeightPosted", "netWeightPosted", "tariffDescription", "shiptoAddressName"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        {'name': 'generalLedgerEntries', 'func': Tables.generalLedgerEntries, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'generalLedgerEntries', 'merge': True, 'mergeKeys': ['id'], 'filter': False, 'filterField': 'lastModifiedDateTime', 'cols': ['id', "entryNo", "gLAccountNo", "postingDate", "documentType", "documentNo", "description", "balAccountNo", "amount", "globalDimension1Code", "globalDimension2Code", "userID", "sourceCode", "priorYearEntry", "quantity", "journalBatchName", "genPostingType", "genBusPostingGroup", "debitAmount", "creditAmount", "documentDate", "externalDocumentNo", "sourceType", "sourceNo", "reversedByEntryNo", "reversedEntryNo", "gLAccountName", "shortcutDimension3Code", "shortcutDimension4Code", "wmsDocumentNo", "wmsDocumentLineNo", 'lastModifiedDateTime', 'noSeries'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsDocumentHeaders', 'func': Tables.wmsDocumentHeaders, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentHeaders', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['id', 'documentType', 'no', 'sellToCustomerNo', 'buildingCode', 'postingDate', 'statusCode', 'containerNo', 'vesselNo', 'senderAddressNo', 'shipToAddressNo', 'billOfLadingNo', 'shortcutDimension2Code', 'attribute04', 'containerSizeCode', 'sellToCustomerName', 'billToCustomerNo', 'billToCustomerName', 'salespersonCode', 'direction', 'locationNo', 'movementType', 'documentDate', 'orderDate', 'statusTemplateCode', 'createdDateTime', 'modifiedDateTime', 'noSeries', 'externalDocumentNo', 'externalReference', 'shippingAgentCode', 'announcedDate', 'arrivedDate', 'departedDate', 'deliveryDate', 'estimatedDepartureDate', 'parentDocumentType', 'parentDocumentNo', 'customsCode', 'tariffNo', 'countryOfOriginCode', 'countryOfDestinationCode', 'declarationDate', 'destination', 'expectQtyCarriers', 'customsValue', 'incotermsCode', 'invoiceValue', 'incotermsCity', 'additionalDocumentNo', 'certificateNo', 'shortcutDimension1Code', 'attribute05', 'grossWeight', 'netWeight', 'quantity', 'carrierQuantity', 'portFrom', 'portTo', 'sealNo', 'sendersAddressName', 'shipToAddressName', 'sendersAddressCity', 'shipToAddressCity', 'carrierQtyCreated', 'consigneeName', 'shipperName', 'agentName'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'customerLedgerEntries', 'func': Tables.customerLedgerEntries, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'customerLedgerEntries', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'postingDate', 'cols': ['id', "entryNo", "customerNo", "postingDate", "documentNo", "description", "originalAmtLCY", "remainingAmtLCY", "customerPostingGroup", "open", "dueDate"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'vendorLedgerEntries', 'func': Tables.vendorLedgerEntries, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'vendorLedgerEntries', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'postingDate', 'cols': ['id', "entryNo", "vendorNo", "postingDate", "documentNo", "description", "originalAmtLCY", "remainingAmtLCY", "amountLCY", "purchaseLCY", "open", "dueDate"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsDocumentContainers', 'func': Tables.wmsDocumentContainers, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentContainers', 'merge': False, 'mergeKeys': ['id'], 'filter': False, 'filterField': '', 'cols': ['id', "documentType", "documentNo", "lineNo", "containerNo", "sizeCode", "loadingAddressNo", "loadingAddressName", "loadingReference", "loadingAddressCity", "unloadingAddressNo", "unloadingReference", "unloadingAddressName", "unloadingAddressCity", "pickUpAddressNo", "pickUpReference", "pickUpAddressName", "pickUpAddressCity", "dropOffAddressNo", "dropOffReference", "dropOffAddressName", "dropOffAddressCity", "loadingDateFrom", "loadingDateTo", "loadingTimeFrom", "loadingTimeTo", "unloadingDateFrom", "unloadingDateTo", "unloadingTimeFrom", "unloadingTimeTo", "inDemurrageDate", "inDetentionDate", "shippingCompanyAddress", "shippingCompanyName", "etdDate", "etdTime", "etaDate", "etaTime", "vesselNo", "vesselname", "billOfLadingNo"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsPostedDocumentHeaders', 'func': Tables.wmsPostedDocumentHeaders, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsPostedDocumentHeaders', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['id', "documentType", "no", "sellToCustomerNo", "sellToCustomerName", "billToCustomerName", "buildingCode", "salespersonCode", "direction", "documentDate", "postingDate", "statusCode", "deliveryDate", "customsCode", "sendersAddressName", "shipToAddressName", "shortcutDimension2Code", "attribute04", "attribute05", 'modifiedDateTime'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsPostedDocumentLines', 'func': Tables.wmsPostedDocumentLines, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsPostedDocumentLines', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['id', 'modifiedDateTime', "documentNo", "lineNo", "sellToCustomerNo", "lineAmountLcy", "no", "description", "quantity", "grossWeight", "netWeight", "containerNo", "vesselNo", "postingDate"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsDocumentPackageLines', 'func': Tables.wmsDocumentPackageLines, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentPackageLines', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['id', 'modifiedDateTime', "documentType", "documentNo", "documentLineNo", "packageCode", "description", "grossWeight", "containerNo", "containerLineNo", "cubage", "numberOfPackages"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'wmsDocumentCommentLines', 'func': Tables.wmsDocumentCommentLines, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentCommentLines', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'date', 'cols': ['*'], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'purchaseHeaders', 'func': Tables.purchaseHeaders, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'purchaseHeaders', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'postingDate', 'cols': ['id', "documentType", "no", "payToVendorNo", "payToName", "yourReference", "orderDate", "postingDate", "purchaserCode", "amount", "amountIncludingVAT", "vendorInvoiceNo", "documentDate"], 'expandCol': None, 'expandColSelect': [], 'pars': None},
        # {'name': 'statusLog', 'func': Tables.statusLog, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'statusLog', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'systemModifiedAt', 'cols': ['id', "systemModifiedAt", "entryNo", "documentNo", "oldStatusCode", "newStatusCode", "userID", 'templateCode'], 'expandCol': None, 'expandColSelect': [], 'pars': None},


        # Bezig


        # {'name': 'customerLedgerEntries', 'func': Tables.check, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'customerLedgerEntries', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': 'modifiedDateTime', 'cols': ['*'], 'expandCol': None, 'expandColSelect': [], 'pars': None},


        # Comments
            #         """
            # Chart_of_Accounts (is pagina, staat in legacy)
            # Status_Steps is Odata, zoeken in API, lijkt iets met verkoopfacturen ofzo
            # WMSDocumentCommentSheet Is deels gelukt, maar niet extended comments want ook weer legacy

            #         """

        # Reference for expand
        # {'name': 'wmsDocumentHeaders', 'func': Tables.check, 'urlDir': 'boltrics/boltrics/v1.0', 'endpoint': 'wmsDocumentHeaders', 'merge': True, 'mergeKeys': ['id'], 'filter': True, 'filterField': None, 'cols': ["documentType", "no", "sellToCustomerNo", "billToCustomerNo", "dossierNo", "buildingCode", "billOfLadingType", "direction", "locationNo", "voyageNo", "timeSlotBookingNo", "timeSlotNeededCapacity", "movementType", "documentDate", "orderDate", "postingDate", "statusCode", "orderpicker", "comment", "docInfoSetID", "externalDocumentNo", "externalReference", "shippingAgentCode", "vehicleNo", "trailerContainerNo", "dockNo", "announcedDate", "announcedTime", "arrivedDate", "arrivedTime", "departedDate", "departedTime", "waitingDuration", "destination", "plannedStartDate", "plannedEndDate", "plannedStartTime", "plannedEndTime", "containerNo", "orderTypeCode", "billOfLadingNo", "senderAddressNo", "shipToAddressNo", "sendersAddressName", "shipToAddressName", "shipToAddressCity", "tripNo", "attribute01", "attribute02", "attribute03", "attribute04", "attribute05", "attribute06", "attribute07", "attribute08", "attribute09", "attribute10", "grossWeight", "netWeight", "quantity", "carrierQuantity", "shipperName"], 'expandCol': 'wmsDocumentLines', 'expandColSelect': ["documentNo", "lineNo", "sellToCustomerNo", "buyFromVendorNo", "dossierNo", "lineAmountLCY", "originalLineQty", "qtyPerCarrier", "type", "no", "itemNo", "description", "unitOfMeasureCode", "baseUnitOfMeasureCode", "quantity", "carrierQtyPosted", "grossWeight", "netWeight", "locationNo", "buildingCode", "tariffDescription", "orderTypeCode", "postingDate", "attribute02", "attribute03", "attribute04", "attribute06", "attribute09", "carrierQuantity"], 'pars': f"""arrivedDate le {(datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")} and arrivedDate ge {(datetime.now()).strftime("%Y-%m-%dT%H:%M:%SZ")}"""},

    ]

    for customer in customers:
        
        s = secrets(vaultUrl='https://bi-prod-keyvault-svnshfg.vault.azure.net/', local=True if app_env == 'local' else False, prefix=customer['credPrefix'])

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

        base_url = f"""https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/api"""

       
        syncLogName = 'syncLog'
        synclog = dict(sqlc.execSingle(f"SELECT tableName, MAX(date) AS date FROM `{dbName}`.{syncLogName} GROUP BY tableName"))


        for t in tbls:
            testing = True if t['func'] == Tables.check else False

            tableName = t['name']
            print(f"Working on {tableName}") if app_env == 'local' else None
            endpoint = f'{t['urlDir']}{'/' if t['urlDir'] else ''}{t['endpoint']}'
            url = f"{base_url}/{endpoint}"




            testing = True
            pp = 221
            for i in range(230):
                tStart = time()
                print(f"Page {pp}")
                resp = get_resp(url, headers=headers, pagination=False if testing else True, addpars=(makeFilter(synclog, tableName, t['filterField'], t.get('pars', None)) if t['filter'] else {}), cols=t['cols'], expandCol=t['expandCol'], expandColSelect=t['expandColSelect'], page=pp)
                pp += 1
                if len(resp) == 0 and testing:
                    print("No more data")
                    break
                df = t['func'](resp)


                try:
                    sqlc.writeMany(df, tableName, dbName, merge=t['merge'], mergeKeys=t['mergeKeys'])
                except Exception as e:
                    print(df.dtypes)
                    print(f"Error in {t['name']} >>>> {e}")
                    logging.exception(e)

                tEnd = time() - tStart
                print(f"Finished in: {tEnd}")




        sqlc.close()
    return




if __name__ == "__main__":
    main()
